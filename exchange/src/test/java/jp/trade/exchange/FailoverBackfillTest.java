package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import io.aeron.cluster.service.Cluster;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.EventSink;
import jp.trade.exchange.events.GapFillingConsumer;
import jp.trade.exchange.events.LogReplayer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * 真实故障场景：leader 把命令提交到多数派后、事件发布前“崩溃”，这些事件永远不会到达下游；
 * 新 leader 只发布之后的事件。下游检测到缺口，从幸存节点的 Raft 日志确定性重放补回。
 */
class FailoverBackfillTest {
    private static final int N = 3;

    private final ExchangeNode[] nodes = new ExchangeNode[N];
    private final File[] dirs = new File[N];
    private final List<byte[]> delivered = Collections.synchronizedList(new ArrayList<>());
    private final List<byte[]> leaderTruth = Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean[] blackhole = {new AtomicBoolean(), new AtomicBoolean(), new AtomicBoolean()};

    private static long seq(byte[] e) { return eventSeq(new UnsafeBuffer(e), 0); }

    /** 补数源：用任意一个存活节点的日志。 */
    private final GapFillingConsumer.Backfill survivorBackfill = new GapFillingConsumer.Backfill() {
        private <T> T withSurvivor(java.util.function.Function<LogReplayer, T> f) {
            for (ExchangeNode n : nodes) {
                if (n != null) {
                    try (LogReplayer r = new LogReplayer(n.clusterDir(), n.aeronDirectoryName(), n.archiveControlEndpoint())) {
                        return f.apply(r);
                    }
                }
            }
            throw new IllegalStateException("no survivor");
        }

        @Override public List<byte[]> fetch(long from, long to) { return withSurvivor(r -> r.fetch(from, to)); }
        @Override public long headSeq() { return withSurvivor(LogReplayer::headSeq); }
    };

    private final GapFillingConsumer consumer = new GapFillingConsumer(0, survivorBackfill, delivered::add);

    private EventSink sinkFor(int member) {
        return (seq, event) -> {
            if (blackhole[member].get()) {
                leaderTruth.add(event);       // 真值：leader 实际产生、但没能发布出去的事件
            } else {
                consumer.publish(seq, event);
            }
        };
    }

    private int awaitLeader() throws Exception {
        final long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            int leader = -1, count = 0;
            for (int i = 0; i < N; i++) {
                if (nodes[i] != null && nodes[i].service().role() == Cluster.Role.LEADER) {
                    leader = i;
                    count++;
                }
            }
            if (count == 1) {
                return leader;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("no leader");
    }

    private void awaitSameHash() throws Exception {
        final long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            String h = null;
            boolean same = true;
            for (ExchangeNode n : nodes) {
                if (n != null) {
                    final String x = n.service().stateHash();
                    same &= h == null || h.equals(x);
                    h = x;
                }
            }
            if (same) {
                return;
            }
            Thread.sleep(100);
        }
        fail("hash did not converge");
    }

    @Test
    void eventsLostAtLeaderCrashAreRecoveredFromSurvivorLog() throws Exception {
        for (int i = 0; i < N; i++) {
            dirs[i] = Files.createTempDirectory("exch-fb" + i).toFile();
            nodes[i] = ExchangeNode.start(i, N, dirs[i], true, sinkFor(i));
        }
        try (ExchangeClient c = ExchangeClient.connect(N)) {
            final int leader1 = awaitLeader();
            c.deposit(1, ASSET_QUOTE, 1_000_000);       // seq 1，正常发布
            c.deposit(2, ASSET_BASE, 1_000);            // seq 2，正常发布
            c.await(2, 15_000);
            awaitSameHash();

            // 模拟“已提交但未发布”：leader 的发布路径断开，但命令照常提交、状态照常推进
            blackhole[leader1].set(true);
            c.place(2, SIDE_SELL, 1, 5_000, 10);        // seq 3
            c.place(1, SIDE_BUY, 1, 5_000, 4);          // seq 4 ACCEPTED, seq 5 TRADE
            c.place(1, SIDE_BUY, 2, 4_000, 7);          // seq 6
            c.await(4, 15_000);
            awaitSameHash();                            // 多数派（此处为全部）已持有这些提交
            assertEquals(2, consumer.lastSeq(), "下游此时只见到 seq 1..2");

            nodes[leader1].close();                     // leader 崩溃
            nodes[leader1] = null;
            awaitLeader();

            // 新 leader 继续服务（超时重试 + clientOrderId 去重，见 FailoverTest）
            boolean ok = false;
            for (int attempt = 0; attempt < 8 && !ok; attempt++) {
                c.place(1, SIDE_BUY, 3, 4_100, 2);
                try {
                    c.await(1, 5_000);
                    ok = true;
                } catch (IllegalStateException timeout) {
                    // 重试
                }
            }
            assertTrue(ok);
            Thread.sleep(500);
            consumer.catchUpToHead();

            // 下游：严格连续，无重复，无缺口
            final long head = delivered.size();
            assertTrue(head >= 7, "事件数 " + head);
            for (int i = 0; i < head; i++) {
                assertEquals(i + 1, seq(delivered.get(i)));
            }
            assertEquals(head, consumer.lastSeq());
            assertTrue(consumer.filled() >= 4, "seq 3..6 由补数填上, filled=" + consumer.filled());

            // 补回来的事件与旧 leader 当时实际产生的逐字节相同（证明确定性重放正确）
            assertEquals(4, leaderTruth.size());
            for (byte[] truth : leaderTruth) {
                assertArrayEquals(truth, delivered.get((int) seq(truth) - 1), "补数事件 seq=" + seq(truth));
            }
            System.out.println("[failover-backfill] delivered=" + head + " filled=" + consumer.filled());
        } finally {
            for (ExchangeNode n : nodes) {
                if (n != null) {
                    n.close();
                }
            }
        }
    }
}
