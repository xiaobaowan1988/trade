package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import io.aeron.cluster.ClusterTool;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.EventsUnavailableException;
import jp.trade.exchange.events.LogReplayer;
import jp.trade.exchange.events.LoggingEventSink;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/** 补数从快照开始重放；日志裁剪后，快照之后的缺口仍可补，更早的缺口给出明确错误。 */
class SnapshotBackfillTest {
    private static long seq(byte[] e) { return eventSeq(new UnsafeBuffer(e), 0); }
    private static int type(byte[] e) { return eventType(new UnsafeBuffer(e), 0); }

    private static void awaitEvents(LoggingEventSink sink, int n) throws Exception {
        final long deadline = System.currentTimeMillis() + 15_000;
        while (sink.events().size() < n && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(sink.events().size() >= n, "events " + sink.events().size() + " < " + n);
    }

    /** 触发快照并等到它出现在 RecordingLog 里。 */
    private static void snapshotAndWait(ExchangeNode node, LogReplayer r, int expectedCount) throws Exception {
        assertTrue(ClusterTool.snapshot(node.clusterDir(), System.out));
        final long deadline = System.currentTimeMillis() + 15_000;
        while (r.snapshotCount() < expectedCount && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(expectedCount, r.snapshotCount());
    }

    private static void placeOrders(ExchangeClient c, int from, int count) {
        for (int i = from; i < from + count; i++) {
            c.place(1, SIDE_BUY, i, 100, 1); // 不会成交的买单：每条命令恰好产生 1 个 ACCEPTED 事件
        }
        c.await(count, 60_000);
    }

    private static void assertSameAsTruth(List<byte[]> got, List<byte[]> truth, long from, long to) {
        assertEquals(to - from + 1, got.size());
        for (int i = 0; i < got.size(); i++) {
            assertArrayEquals(truth.get((int) from - 1 + i), got.get(i), "seq " + (from + i));
        }
    }

    @Test
    void seedsFromLatestSnapshotAndFallsBackToLogStart() throws Exception {
        final File dir = Files.createTempDirectory("exch-sb1").toFile();
        final LoggingEventSink sink = new LoggingEventSink(false);
        try (ExchangeNode node = ExchangeNode.start(dir, true, sink);
             LogReplayer r = new LogReplayer(node.clusterDir(), node.aeronDirectoryName(), node.archiveControlEndpoint());
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            c.deposit(1, ASSET_QUOTE, 1_000_000_000L);
            c.await(1, 10_000);
            placeOrders(c, 1, 40);                 // seq 1..41
            awaitEvents(sink, 41);
            snapshotAndWait(node, r, 1);
            placeOrders(c, 41, 40);                // seq 42..81
            awaitEvents(sink, 81);
            final List<byte[]> truth = sink.events();

            // 请求快照之后的区间：用快照做种子
            assertSameAsTruth(r.fetch(50, 70), truth, 50, 70);
            assertTrue(r.lastSeed().startsWith("snapshot@"), r.lastSeed());
            assertTrue(r.lastSeed().contains("lastSeq=41"), r.lastSeed());

            // 请求快照之前的区间：不能用该快照，回退到日志起点
            assertSameAsTruth(r.fetch(5, 30), truth, 5, 30);
            assertEquals("log-start", r.lastSeed());

            // 恰好跨过快照边界：fromSeq=42 → 种子是 lastSeq=41 的快照
            assertSameAsTruth(r.fetch(42, 81), truth, 42, 81);
            assertTrue(r.lastSeed().contains("lastSeq=41"), r.lastSeed());

            // 日志头：用最新快照做种子，状态与线上服务逐字节一致
            assertEquals(81, r.headSeq());
            Thread.sleep(300);
            assertEquals(node.service().stateHash(), r.rebuildState().stateHash());
            assertTrue(r.lastSeed().startsWith("snapshot@"));
        }
    }

    @Test
    void afterTrimmingLogOnlyPostSnapshotEventsAreRecoverable() throws Exception {
        final File dir = Files.createTempDirectory("exch-sb2").toFile();
        // 把归档 segment 缩到 64KB（与日志 term 同长），几千条命令就能跨多个 segment
        final java.util.function.Consumer<io.aeron.samples.cluster.ClusterConfig> tiny =
                cfg -> {
                    cfg.consensusModuleContext().logChannel("aeron:ipc?term-length=64k");
                    cfg.archiveContext().segmentFileLength(64 * 1024);
                };

        final LoggingEventSink sink = new LoggingEventSink(false);
        long s2Seq;
        long total;
        try (ExchangeNode node = ExchangeNode.start(0, 1, dir, true, sink, tiny);
             LogReplayer r = new LogReplayer(node.clusterDir(), node.aeronDirectoryName(), node.archiveControlEndpoint());
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            c.deposit(1, ASSET_QUOTE, 1_000_000_000_000L);
            c.await(1, 10_000);
            placeOrders(c, 1, 1_200);                       // seq 2..1201
            awaitEvents(sink, 1_201);
            snapshotAndWait(node, r, 1);
            placeOrders(c, 1_201, 1_200);                   // seq 1202..2401
            awaitEvents(sink, 2_401);
            snapshotAndWait(node, r, 2);
            s2Seq = 2_401;                                  // 第二个快照的最后事件序号
            placeOrders(c, 2_401, 50);                      // seq 2402..2451
            total = 2_451;
            awaitEvents(sink, (int) total);
            Thread.sleep(300);
            final List<byte[]> truth = sink.events();

            // 裁剪前：任意区间都能补
            assertSameAsTruth(r.fetch(10, 20), truth, 10, 20);

            final long newStart = r.trimLogToLatestSnapshot();
            assertTrue(newStart > 0, "日志起点应被推进, start=" + newStart);

            // 快照之后的区间：仍可补，且确实用了快照
            assertSameAsTruth(r.fetch(s2Seq + 1, total), truth, s2Seq + 1, total);
            assertTrue(r.lastSeed().contains("lastSeq=" + s2Seq), r.lastSeed());

            // 更早的区间：本地已无法重新生成，错误里给出最早可恢复序号
            final EventsUnavailableException ex = assertThrows(EventsUnavailableException.class, () -> r.fetch(10, 20));
            assertTrue(ex.earliestRecoverableSeq > 20, "earliest=" + ex.earliestRecoverableSeq);
            final long earliest = ex.earliestRecoverableSeq;
            assertSameAsTruth(r.fetch(earliest, total), truth, earliest, total);
            assertThrows(EventsUnavailableException.class, () -> r.fetch(earliest - 1, total));

            // 状态一致：裁剪后的日志 + 快照仍能重建与线上完全相同的状态
            assertEquals(node.service().stateHash(), r.rebuildState().stateHash());
        }

        // 集群本身也能从“被裁剪的日志 + 快照”重启恢复：撤销一个早期挂单
        final LoggingEventSink sink2 = new LoggingEventSink(false);
        try (ExchangeNode node = ExchangeNode.start(0, 1, dir, false, sink2, tiny);
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            assertTrue(node.service().loadedFromSnapshot());
            c.cancel(1, 2); // order 1 是 seq 2 的那张买单
            final List<byte[]> resp = c.await(1, 15_000);
            assertEquals(EVT_CANCELLED, type(resp.get(0)));
            assertEquals(seq(resp.get(0)), total + 1, "事件序号在重启后接着往后走");
        }
    }
}
