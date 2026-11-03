package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import io.aeron.cluster.service.Cluster;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.LoggingEventSink;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * 链式哈希检查点：把“序号 + 对全部输出事件的哈希”写进 Raft 日志，任何重放都必须算出同一个值。
 * 覆盖：正常运行与重启重放都能通过核对；新代码语义与旧日志不一致、副本分歧时 fail-stop；客户端伪造不了检查点。
 */
class ChainCheckpointTest {
    private static void orders(ExchangeClient c, int n) {
        c.deposit(1, ASSET_QUOTE, 1_000_000_000_000L);
        for (int i = 1; i <= n; i++) {
            c.place(1, SIDE_BUY, i, 100, 1); // 每条命令恰好一个事件
        }
        c.await(n + 1, 120_000);
    }

    private static void awaitTrue(java.util.function.BooleanSupplier cond, long ms, String msg) throws Exception {
        final long deadline = System.currentTimeMillis() + ms;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(cond.getAsBoolean(), msg);
    }

    @Test
    void normalRunAndRestartReplayBothPassTheCheckpointCheck() throws Exception {
        final File dir = Files.createTempDirectory("cc1").toFile();
        try (ExchangeNode node = ExchangeNode.start(dir, true, new LoggingEventSink(false));
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            orders(c, 2_500);                         // 2501 个事件 → 检查点在 1000、2000
            awaitTrue(() -> node.service().checkpointsVerified() >= 2, 10_000, "运行中的检查点被核对通过");
            assertEquals(0, node.service().checkpointsUnverifiable());
            assertFalse(node.service().halted());
        }
        // 重启：整段日志重放，重放期间必须再次核对通过（而不是悄悄跳过）
        try (ExchangeNode node = ExchangeNode.start(dir, false, new LoggingEventSink(false))) {
            awaitTrue(() -> node.service().checkpointsVerified() >= 2, 15_000, "重放时重新核对了日志里的检查点");
            assertFalse(node.service().halted());
        }
    }

    @Test
    void replayingTheLogWithChangedSemanticsFailsStopInsteadOfSilentlyProducingAnotherState() throws Exception {
        final File dir = Files.createTempDirectory("cc2").toFile();
        try (ExchangeNode node = ExchangeNode.start(dir, true, new LoggingEventSink(false));
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            orders(c, 1_500);
            awaitTrue(() -> node.service().checkpointsVerified() >= 1, 10_000, "日志里已有检查点");
        }
        // “新版本代码”对同一份日志得到不同的结果（这里用订单号编码变化模拟），且没有快照可供版本校验
        final List<Throwable> fatals = new CopyOnWriteArrayList<>();
        try (ExchangeNode node = ExchangeNode.start(0, 1, dir, false, new LoggingEventSink(false), cfg -> {},
                svc -> svc.fatalHandler(fatals::add).skewForTest(7))) {
            awaitTrue(() -> !fatals.isEmpty(), 20_000, "重放到第一个检查点时必须 fail-stop");
            assertTrue(fatals.get(0).getCause().getMessage().contains("checkpoint mismatch"), String.valueOf(fatals.get(0).getCause()));
            assertTrue(node.service().halted());
            assertEquals(0, node.service().checkpointsVerified());
        }
    }

    @Test
    void aReplicaWhoseReplayDivergesStopsWhileTheOthersKeepServing() throws Exception {
        final int n = 3;
        final File[] dirs = new File[n];
        final ExchangeNode[] nodes = new ExchangeNode[n];
        final List<Throwable> fatal2 = new CopyOnWriteArrayList<>();
        for (int i = 0; i < n; i++) {
            dirs[i] = Files.createTempDirectory("cc3-" + i).toFile();
        }
        try {
            // 先只启动 0、1 两个节点（多数派），让它们选出 leader 并产生日志里的检查点
            for (int i = 0; i < 2; i++) {
                // Aeron 冷启动时默认要等 60 秒的“启动探查”才会在缺席成员的情况下用多数派选主；测试里缩短
                nodes[i] = ExchangeNode.start(i, n, dirs[i], true, new LoggingEventSink(false),
                        cfg -> cfg.consensusModuleContext().startupCanvassTimeoutNs(java.util.concurrent.TimeUnit.SECONDS.toNanos(4)));
            }
            awaitTrue(() -> nodes[0].service().role() == Cluster.Role.LEADER || nodes[1].service().role() == Cluster.Role.LEADER,
                    30_000, "两个节点选出了 leader");
            try (ExchangeClient c = ExchangeClient.connect(n)) {
                orders(c, 1_500);
                awaitTrue(() -> nodes[0].service().checkpointsVerified() >= 1 && nodes[1].service().checkpointsVerified() >= 1,
                        15_000, "两个正常副本都核对通过");

                // 节点 2 加入，它的重放结果与日志不一致（语义分歧）：必须自己停下
                nodes[2] = ExchangeNode.start(2, n, dirs[2], true, new LoggingEventSink(false),
                        cfg -> cfg.consensusModuleContext().startupCanvassTimeoutNs(java.util.concurrent.TimeUnit.SECONDS.toNanos(4)),
                        svc -> svc.fatalHandler(fatal2::add).skewForTest(1_000));
                awaitTrue(() -> !fatal2.isEmpty(), 30_000, "分歧的副本 fail-stop");
                assertTrue(fatal2.get(0).getCause().getMessage().contains("checkpoint mismatch"));

                // 正常副本不受影响，集群继续正确服务
                assertFalse(nodes[0].service().halted());
                assertFalse(nodes[1].service().halted());
                c.place(1, SIDE_BUY, 9_999, 100, 1);
                boolean ok = false;
                for (int attempt = 0; attempt < 6 && !ok; attempt++) {
                    try {
                        c.await(1, 5_000);
                        ok = true;
                    } catch (IllegalStateException timeout) {
                        c.place(1, SIDE_BUY, 9_999, 100, 1); // 同一个 clientOrderId，幂等重试
                    }
                }
                assertTrue(ok, "剩下的两个正常副本继续服务");
            }
        } finally {
            for (ExchangeNode node : nodes) {
                if (node != null) {
                    node.close();
                }
            }
        }
    }

    @Test
    void aClientCannotForgeACheckpointToHaltTheCluster() throws Exception {
        final File dir = Files.createTempDirectory("cc4").toFile();
        try (ExchangeNode node = ExchangeNode.start(0, 1, dir, true, new LoggingEventSink(false), cfg -> {},
                svc -> svc.fatalHandler(t -> fail("伪造的检查点不应触发 fail-stop: " + t)));
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            orders(c, 1_200);
            // 构造一条看起来像检查点的消息：魔数 + 序号 1000 + 错误的哈希
            final ByteBuffer forged = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
            forged.putInt(0x4E584348).putLong(1_000).putLong(0xDEADBEEFL);
            c.sendRawForTest(forged.array());
            final byte[] r = c.await(1, 10_000).get(0);
            assertEquals(EVT_REJECTED, eventType(new UnsafeBuffer(r), 0), "当作畸形命令被拒绝");
            assertEquals(REJECT_MALFORMED, eventQty(new UnsafeBuffer(r), 0));
            assertFalse(node.service().halted());
            assertEquals(Cluster.Role.LEADER, node.service().role());
        }
    }

    /**
     * 日志里夹着服务自己发的检查点消息。重放（补数、对账、重建状态）如果把它们当命令交给引擎，
     * 引擎会产生多余的“畸形命令”事件，让重放出的事件序号和线上错位，补数就悄悄给出了错误的事件。
     */
    @Test
    void replayedEventsMatchLiveEventsExactlyAndTheReplayVerifiesTheCheckpointsToo() throws Exception {
        final File dir = Files.createTempDirectory("cc5").toFile();
        final LoggingEventSink sink = new LoggingEventSink(false);
        try (ExchangeNode node = ExchangeNode.start(dir, true, sink);
             jp.trade.exchange.events.LogReplayer r = new jp.trade.exchange.events.LogReplayer(
                     node.clusterDir(), node.aeronDirectoryName(), node.archiveControlEndpoint());
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            orders(c, 2_500);
            // 客户端伪造的“检查点样子”的消息：线上当作畸形命令产生一个 REJECTED 事件；重放必须同样产生，不能当成检查点跳过
            final ByteBuffer forged = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
            forged.putInt(0x4E584348).putLong(1_000).putLong(0xDEADBEEFL);
            c.sendRawForTest(forged.array());
            c.await(1, 10_000);
            orders2(c, 1_000);
            awaitTrue(() -> sink.events().size() >= 3_502, 20_000, "事件都已发布");
            Thread.sleep(300);

            final long head = sink.events().size();
            final List<byte[]> replayed = r.fetch(1, head);
            assertEquals(head, replayed.size(), "重放出的事件数与线上一致（没有多出检查点造成的畸形命令事件）");
            for (int i = 0; i < head; i++) {
                assertArrayEquals(sink.events().get(i), replayed.get(i), "event " + (i + 1));
            }
            assertTrue(r.checkpointsVerified() >= 3, "重放时也核对了日志里的检查点: " + r.checkpointsVerified());
            assertEquals(node.service().stateHash(), r.rebuildState().stateHash(), "状态（含检查点边界）与线上一致");
        }
    }

    private static void orders2(ExchangeClient c, int n) {
        for (int i = 10_000; i < 10_000 + n; i++) {
            c.place(1, SIDE_BUY, i, 100, 1);
        }
        c.await(n, 60_000);
    }
}
