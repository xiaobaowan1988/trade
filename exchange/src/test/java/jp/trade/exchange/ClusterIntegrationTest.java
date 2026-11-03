package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import io.aeron.cluster.ClusterTool;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.LoggingEventSink;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/** 真实 Aeron 集群（1 节点 Raft 组）集成测试：定序、撮合、事件出口、重启恢复、快照恢复。 */
class ClusterIntegrationTest {
    private static int type(byte[] e) { return eventType(new UnsafeBuffer(e), 0); }
    private static long qty(byte[] e) { return eventQty(new UnsafeBuffer(e), 0); }
    private static long seq(byte[] e) { return eventSeq(new UnsafeBuffer(e), 0); }

    private static void awaitEvents(LoggingEventSink sink, int n) throws Exception {
        final long deadline = System.currentTimeMillis() + 10_000;
        while (sink.events().size() < n && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(sink.events().size() >= n, "events " + sink.events().size() + " < " + n);
    }

    @Test
    void matchesThroughClusterAndPublishesOrderedEvents() throws Exception {
        final File dir = Files.createTempDirectory("exch-it1").toFile();
        final LoggingEventSink sink = new LoggingEventSink(false);
        try (ExchangeNode node = ExchangeNode.start(dir, true, sink);
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            c.deposit(1, ASSET_QUOTE, 1_000_000);
            c.deposit(2, ASSET_BASE, 100);
            c.await(2, 10_000);
            c.place(2, SIDE_SELL, 1, 5_000, 10);
            c.place(1, SIDE_BUY, 1, 5_000, 10);
            final List<byte[]> r = c.await(3, 10_000);
            assertEquals(EVT_ACCEPTED, type(r.get(0)));
            assertEquals(EVT_ACCEPTED, type(r.get(1)));
            assertEquals(EVT_TRADE, type(r.get(2)));
            assertEquals(10, qty(r.get(2)));

            awaitEvents(sink, 5);
            for (int i = 0; i < 5; i++) {
                assertEquals(i + 1, seq(sink.events().get(i)), "事件序号连续");
            }
            assertEquals(0, node.bridge().deduper().gaps());
            assertEquals(0, node.service().droppedEvents());
        }
    }

    @Test
    void recoversFromLogReplayAfterRestart() throws Exception {
        final File dir = Files.createTempDirectory("exch-it2").toFile();
        final LoggingEventSink sink1 = new LoggingEventSink(false);
        try (ExchangeNode node = ExchangeNode.start(dir, true, sink1);
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            c.deposit(1, ASSET_QUOTE, 1_000_000);
            c.place(1, SIDE_BUY, 1, 100, 5); // order 1 挂单
            c.await(2, 10_000);
        }
        // 重启，不清数据：状态必须由 Raft 日志重放恢复
        final LoggingEventSink sink2 = new LoggingEventSink(false);
        try (ExchangeNode node = ExchangeNode.start(dir, false, sink2);
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            c.cancel(1, 1);
            final List<byte[]> r = c.await(1, 10_000);
            assertEquals(EVT_CANCELLED, type(r.get(0)), "重放后订单 1 仍在簿内");
            assertEquals(5, qty(r.get(0)));
            c.place(1, SIDE_BUY, 1, 100, 1); // clientOrderId=1 已用过 → 重放后的去重集合也被恢复
            assertEquals(REJECT_DUPLICATE_CLIENT_ORDER, qty(c.await(1, 10_000).get(0)));
            System.out.println("[replay] 重启后新事件序号起点: "
                    + (sink2.events().isEmpty() ? "none" : seq(sink2.events().get(0))));
            Thread.sleep(300);
            System.out.println("[replay] sink2 events=" + sink2.events().size()
                    + " gaps=" + node.bridge().deduper().gaps());
        }
    }

    @Test
    void recoversFromSnapshot() throws Exception {
        final File dir = Files.createTempDirectory("exch-it3").toFile();
        try (ExchangeNode node = ExchangeNode.start(dir, true, new LoggingEventSink(false));
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            c.deposit(1, ASSET_QUOTE, 1_000_000);
            c.place(1, SIDE_BUY, 1, 100, 5);
            c.await(2, 10_000);
            assertTrue(ClusterTool.snapshot(node.clusterDir(), System.out), "snapshot 请求被接受");
            final long deadline = System.currentTimeMillis() + 10_000;
            while (!ClusterTool.describeLatestConsensusModuleSnapshot(System.out, node.clusterDir()) && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            Thread.sleep(500);
        }
        try (ExchangeNode node = ExchangeNode.start(dir, false, new LoggingEventSink(false));
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            assertTrue(node.service().loadedFromSnapshot(), "应从快照启动而非仅重放日志");
            c.cancel(1, 1);
            assertEquals(EVT_CANCELLED, type(c.await(1, 10_000).get(0)));
        }
    }
}
