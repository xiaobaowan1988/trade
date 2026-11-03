package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.EventSink;
import jp.trade.exchange.events.GapFillingConsumer;
import jp.trade.exchange.events.LogReplayer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/** “可以慢，不能丢”：下游慢或失败时，事件不能无声消失。 */
class NoLossClusterTest {
    private static long seq(byte[] e) { return eventSeq(new UnsafeBuffer(e), 0); }

    private static void sendOrders(ExchangeClient c, int n) {
        c.deposit(1, ASSET_QUOTE, 1_000_000_000_000L);
        for (int i = 1; i <= n; i++) {
            c.place(1, SIDE_BUY, i, 100, 1); // 不成交的买单：每条命令恰好一个事件
        }
        c.await(n + 1, 120_000);
    }

    /**
     * 模拟真实下游：publish 只是“发出去”，flush 成功才算“被确认送达”；flush 失败时在途的数据就丢了（像 Kafka 发送失败）。
     * 可选：前 failFlushes 次 flush 失败、或在 gate 打开前阻塞 publish。
     */
    static final class TestSink implements EventSink {
        final List<byte[]> received = Collections.synchronizedList(new ArrayList<>());
        private final List<byte[]> pending = new ArrayList<>();
        final AtomicInteger flushes = new AtomicInteger();
        volatile int failFlushes;
        volatile CountDownLatch gate;

        @Override
        public void publish(long seq, byte[] event) {
            final CountDownLatch g = gate;
            if (g != null) {
                try {
                    g.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            synchronized (pending) {
                pending.add(event);
            }
        }

        @Override
        public void flush() {
            synchronized (pending) {
                if (flushes.getAndIncrement() < failFlushes) {
                    pending.clear(); // 发送失败：在途数据没有送达
                    throw new IllegalStateException("downstream unavailable");
                }
                received.addAll(pending);
                pending.clear();
            }
        }

        long distinctContiguousUpTo() {
            final TreeSet<Long> seqs = new TreeSet<>();
            synchronized (received) {
                for (byte[] e : received) {
                    seqs.add(seq(e));
                }
            }
            long expect = 1;
            for (long s : seqs) {
                if (s != expect) {
                    break;
                }
                expect++;
            }
            return expect - 1;
        }
    }

    private static void awaitContiguous(TestSink sink, long n, long timeoutMs) throws Exception {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (sink.distinctContiguousUpTo() < n && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(n, sink.distinctContiguousUpTo());
    }

    @Test
    void bridgeRetriesFailedDownstreamAcksAndDeliversEverything() throws Exception {
        final TestSink sink = new TestSink();
        sink.failFlushes = 4; // 下游前 4 次确认失败
        try (ExchangeNode node = ExchangeNode.start(new File(Files.createTempDirectory("nl1").toString()), true, sink);
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            sendOrders(c, 300);
            awaitContiguous(sink, 301, 30_000);
            assertTrue(node.bridge().sinkFailures() >= 4, "失败被记录而不是吞掉: " + node.bridge().sinkFailures());
            assertEquals(0, node.service().droppedEvents(), "没有丢弃，只是重试");
            assertNotNull(node.bridge().lastError());
        }
    }

    @Test
    void serviceWaitsInsteadOfDroppingWhenDownstreamIsSlow() throws Exception {
        final TestSink sink = new TestSink();
        final CountDownLatch gate = new CountDownLatch(1);
        sink.gate = gate; // 下游完全卡住
        final int n = 3_000; // 远多于 64KB 的 IPC 缓冲能容纳的事件数
        try (ExchangeNode node = ExchangeNode.start(0, 1, new File(Files.createTempDirectory("nl2").toString()), true, sink,
                cfg -> {}, svc -> svc.eventChannel("aeron:ipc?term-length=64k").eventWait(30_000_000_000L, 30_000_000_000L));
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            final Thread opener = new Thread(() -> {
                try {
                    Thread.sleep(2_500);
                } catch (InterruptedException ignored) {
                }
                gate.countDown(); // 2.5 秒后下游恢复
            });
            opener.start();
            final long t0 = System.nanoTime();
            sendOrders(c, n);
            final double secs = (System.nanoTime() - t0) / 1e9;
            awaitContiguous(sink, n + 1, 30_000);

            assertEquals(0, node.service().droppedEvents(), "下游只是慢：必须等，不能丢");
            assertTrue(node.service().eventWaits() > 0, "撮合线程确实进入了背压等待");
            assertTrue(secs >= 2.0, "整体被拖慢到下游恢复之后（慢，但没丢）: " + secs);
            assertEquals(0, node.bridge().deduper().gaps());
            System.out.printf("[no-loss] 下游卡 2.5s：等待次数=%d，累计等待=%.1fs，丢弃=0，用时 %.1fs%n",
                    node.service().eventWaits(), node.service().eventWaitedNanos() / 1e9, secs);
        }
    }

    @Test
    void whenWaitingIsImpossibleDropsAreCountedBoundedAndRecoveredFromTheLog() throws Exception {
        final File dir = Files.createTempDirectory("nl3").toFile();
        final TestSink downstream = new TestSink();
        final CountDownLatch gate = new CountDownLatch(1);
        downstream.gate = gate; // 下游卡住，而且这次等不到恢复
        final LogReplayer[] replayer = new LogReplayer[1];
        final GapFillingConsumer.Backfill lazy = new GapFillingConsumer.Backfill() {
            @Override public List<byte[]> fetch(long from, long to) { return replayer[0].fetch(from, to); }
            @Override public long headSeq() { return replayer[0].headSeq(); }
        };
        final List<byte[]> delivered = new ArrayList<>();
        final GapFillingConsumer consumer = new GapFillingConsumer(0, lazy, delivered::add);
        final EventSink toConsumer = (s, e) -> {
            downstream.publish(s, e); // 先卡在这里
            consumer.publish(s, e);
        };
        final int n = 3_000;
        try (ExchangeNode node = ExchangeNode.start(0, 1, dir, true, toConsumer, cfg -> {},
                svc -> svc.eventChannel("aeron:ipc?term-length=64k").eventWait(200_000_000L, 60_000_000_000L));
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            replayer[0] = new LogReplayer(node.clusterDir(), node.aeronDirectoryName(), node.archiveControlEndpoint());
            final long t0 = System.nanoTime();
            sendOrders(c, n);
            final double secs = (System.nanoTime() - t0) / 1e9;
            gate.countDown(); // 事后下游恢复
            Thread.sleep(1_000);

            assertTrue(node.service().droppedEvents() > 0, "等不了的时候确实丢了：" + node.service().droppedEvents());
            assertEquals(1, node.service().degradedEntries(), "只进入一次降级窗口，没有每条命令都被拖住一个超时");
            assertTrue(secs < 30, "有界：没有 " + n + "×200ms 那样的灾难性拖慢: " + secs);

            // 但是丢的东西能补回来：下游发现缺口 → 从 Raft 日志确定性重放
            consumer.catchUpToHead();
            assertEquals(n + 1, delivered.size());
            for (int i = 0; i < delivered.size(); i++) {
                assertEquals(i + 1, seq(delivered.get(i)), "下游序号严格连续");
            }
            System.out.printf("[no-loss] 下游死亡：丢弃 %d 条，全部由日志补回，下游最终收到 %d/%d，用时 %.1fs%n",
                    node.service().droppedEvents(), delivered.size(), n + 1, secs);
            replayer[0].close();
        }
    }
}
