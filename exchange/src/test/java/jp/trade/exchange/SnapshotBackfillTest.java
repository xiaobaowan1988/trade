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

/** 补数从快照开始重放（日志裁剪与归档相关的场景见 ArchiveTest）。 */
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
}
