package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import io.aeron.cluster.service.Cluster;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.LoggingEventSink;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/** 3 成员 Raft 组：leader 宕机后选主、状态不丢、旧 leader 重启后追平。 */
class FailoverTest {
    private static final int N = 3;

    private final ExchangeNode[] nodes = new ExchangeNode[N];
    private final LoggingEventSink[] sinks = new LoggingEventSink[N];
    private final File[] dirs = new File[N];

    private static int type(byte[] e) { return eventType(new UnsafeBuffer(e), 0); }
    private static long qty(byte[] e) { return eventQty(new UnsafeBuffer(e), 0); }

    private void startNode(int i, boolean fresh) {
        sinks[i] = new LoggingEventSink(false);
        nodes[i] = ExchangeNode.start(i, N, dirs[i], fresh, sinks[i]);
    }

    private int awaitLeader() throws Exception {
        final long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            int leader = -1;
            int count = 0;
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
        throw new AssertionError("no single leader elected");
    }

    private void awaitSameHash(int... members) throws Exception {
        final long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                final String h = nodes[members[0]].service().stateHash();
                boolean same = true;
                for (int m : members) {
                    same &= h.equals(nodes[m].service().stateHash());
                }
                if (same) {
                    return;
                }
            } catch (RuntimeException racy) {
                // 撮合线程还在改状态时从测试线程读哈希可能读到半截；稳定后再比较
            }
            Thread.sleep(100);
        }
        fail("state hashes did not converge");
    }

    @Test
    void survivesLeaderCrashAndRejoin() throws Exception {
        for (int i = 0; i < N; i++) {
            dirs[i] = Files.createTempDirectory("exch-fo" + i).toFile();
        }
        for (int i = 0; i < N; i++) {
            startNode(i, true);
        }
        try (ExchangeClient c = ExchangeClient.connect(N)) {
            final int leader1 = awaitLeader();
            c.deposit(1, ASSET_QUOTE, 1_000_000);
            c.place(1, SIDE_BUY, 1, 100, 5); // order 1 挂单
            c.await(2, 15_000);
            awaitSameHash(0, 1, 2);          // 三个副本一致

            // ---- leader 宕机 ----
            nodes[leader1].close();
            nodes[leader1] = null;
            final int leader2 = awaitLeader();
            assertNotEquals(leader1, leader2);

            // 切换期间发出的命令可能写进已宕机 leader 的缓冲而丢失 → 客户端必须超时重试。
            // 重试用同一个 clientOrderId，状态机去重保证不会重复下单。
            List<byte[]> r = null;
            for (int attempt = 0; attempt < 8 && r == null; attempt++) {
                c.place(1, SIDE_BUY, 2, 100, 3);
                try {
                    r = c.await(1, 5_000);
                } catch (IllegalStateException timeout) {
                    // 重试
                }
            }
            assertNotNull(r, "切换后命令应最终得到确认");
            final int t = type(r.get(0));
            assertTrue(t == EVT_ACCEPTED || t == EVT_REJECTED, "ACCEPTED，或因首次已生效而被去重拒绝");
            if (t == EVT_REJECTED) {
                assertEquals(REJECT_DUPLICATE_CLIENT_ORDER, qty(r.get(0)));
                assertTrue(eventOrderId(new UnsafeBuffer(r.get(0)), 0) > 0, "重试拿到的是原订单号，丢了回报也不丢结果");
            }

            // 撤销宕机前挂的 order 1：证明已提交的状态没丢
            c.cancel(1, 1);
            r = c.await(1, 15_000);
            assertEquals(EVT_CANCELLED, type(r.get(0)));
            assertEquals(5, qty(r.get(0)));

            final int[] survivors = new int[N - 1];
            for (int i = 0, k = 0; i < N; i++) {
                if (nodes[i] != null) {
                    survivors[k++] = i;
                }
            }
            awaitSameHash(survivors);

            // ---- 旧 leader 重启，从自己的日志恢复并追平 ----
            startNode(leader1, false);
            awaitSameHash(0, 1, 2);
            assertEquals(Cluster.Role.FOLLOWER, nodes[leader1].service().role());
        } finally {
            for (ExchangeNode n : nodes) {
                if (n != null) {
                    n.close();
                }
            }
        }
    }
}
