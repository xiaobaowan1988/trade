package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.aeron.cluster.ClusterTool;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Consumer;
import jp.trade.exchange.archive.ArchiveCatalog;
import jp.trade.exchange.archive.ArchiveKeys;
import jp.trade.exchange.archive.LogArchiver;
import jp.trade.exchange.archive.LogTrimmer;
import jp.trade.exchange.archive.ObjectStore;
import jp.trade.exchange.archive.TrimRefusedException;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.EventsUnavailableException;
import jp.trade.exchange.events.LogReplayer;
import jp.trade.exchange.events.LoggingEventSink;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/** 真实 Aeron 集群 + 真实 MinIO：归档、受保护的裁剪、从对象存储补数。 */
class ArchiveTest {
    private static final Consumer<io.aeron.samples.cluster.ClusterConfig> TINY = cfg -> {
        cfg.consensusModuleContext().logChannel("aeron:ipc?term-length=64k"); // 日志 term 与 segment 都缩到 64KB
        cfg.archiveContext().segmentFileLength(64 * 1024);
    };

    private static int type(byte[] e) { return eventType(new UnsafeBuffer(e), 0); }
    private static long seq(byte[] e) { return eventSeq(new UnsafeBuffer(e), 0); }

    /** 单节点，跑出：1 笔入金 + 1200 单 → 快照1 → 1200 单 → 快照2 → 50 单。 */
    private static final class Fixture implements AutoCloseable {
        static final long S1_SEQ = 1_201;
        static final long S2_SEQ = 2_401;
        static final long TOTAL = 2_451;

        final File dir;
        final LoggingEventSink sink = new LoggingEventSink(false);
        final ExchangeNode node;
        final ObjectStore store;
        final String prefix;
        final ArchiveCatalog catalog;
        final LogReplayer localOnly;
        final LogReplayer withS3;
        final LogArchiver archiver;
        final LogTrimmer trimmer;
        final ExchangeClient client;

        Fixture() throws Exception {
            dir = Files.createTempDirectory("exch-arch").toFile();
            store = TestMinio.store("exchange-archive");
            prefix = "cluster-" + System.nanoTime() + "/member-0";
            catalog = new ArchiveCatalog(store, prefix);
            node = ExchangeNode.start(0, 1, dir, true, sink, TINY);
            localOnly = new LogReplayer(node.clusterDir(), node.aeronDirectoryName(), node.archiveControlEndpoint());
            withS3 = new LogReplayer(node.clusterDir(), node.aeronDirectoryName(), node.archiveControlEndpoint(), catalog);
            archiver = new LogArchiver(withS3, catalog, node.archiveDir());
            trimmer = new LogTrimmer(withS3, catalog, node.archiveDir());
            client = new ExchangeClient(node.aeronDirectoryName());

            client.deposit(1, ASSET_QUOTE, 1_000_000_000_000L);
            client.await(1, 10_000);
            orders(1, 1_200);
            awaitEvents(S1_SEQ);
            snapshot(1);
            orders(1_201, 1_200);
            awaitEvents(S2_SEQ);
            snapshot(2);
            orders(2_401, 50);
            awaitEvents(TOTAL);
            Thread.sleep(300);
        }

        void orders(int from, int count) {
            for (int i = from; i < from + count; i++) {
                client.place(1, SIDE_BUY, i, 100, 1); // 不成交的买单：每条命令恰好一个 ACCEPTED 事件
            }
            client.await(count, 60_000);
        }

        void awaitEvents(long n) throws Exception {
            final long deadline = System.currentTimeMillis() + 15_000;
            while (sink.events().size() < n && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertTrue(sink.events().size() >= n);
        }

        void snapshot(int expected) throws Exception {
            assertTrue(ClusterTool.snapshot(node.clusterDir(), System.out));
            final long deadline = System.currentTimeMillis() + 15_000;
            while (withS3.snapshotCount() < expected && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(expected, withS3.snapshotCount());
        }

        List<byte[]> truth() { return sink.events(); }

        long recordingId() { return withS3.logInfo().recordingId(); }

        @Override
        public void close() {
            client.close();
            withS3.close();
            localOnly.close();
            node.close();
        }
    }

    private static void assertSameAsTruth(List<byte[]> got, List<byte[]> truth, long from, long to) {
        assertEquals(to - from + 1, got.size());
        for (int i = 0; i < got.size(); i++) {
            assertArrayEquals(truth.get((int) from - 1 + i), got.get(i), "seq " + (from + i));
        }
    }

    @Test
    void archiveThenTrimThenBackfillBeyondTheLocalWindow() throws Exception {
        assumeTrue(TestMinio.available(), "需要 minio 可执行文件，见 README");
        try (Fixture f = new Fixture()) {
            // 没归档就想裁剪：必须被拒绝
            assertThrows(TrimRefusedException.class, f.trimmer::trim);

            final LogArchiver.Report first = f.archiver.archiveOnce();
            assertTrue(first.segmentsUploaded() >= 4, "segments=" + first.segmentsUploaded());
            assertEquals(2, first.snapshotsUploaded());
            final LogArchiver.Report again = f.archiver.archiveOnce();
            assertEquals(0, again.segmentsUploaded(), "幂等：第二次不再上传");
            assertEquals(0, again.snapshotsUploaded());

            // 对象存储里的 segment 与本地文件逐字节相同
            final long rec = f.recordingId();
            final byte[] remote = f.store.get(ArchiveKeys.segment(f.prefix, rec, 0));
            assertArrayEquals(java.nio.file.Files.readAllBytes(new File(f.node.archiveDir(), rec + "-0.rec").toPath()), remote);

            final long newStart = f.trimmer.trim();
            assertTrue(newStart > 0, "归档之后才允许裁剪, start=" + newStart);
            assertFalse(new File(f.node.archiveDir(), rec + "-0.rec").exists(), "本地 segment 已删除");

            final List<byte[]> truth = f.truth();

            // 只有本地数据：回溯窗口被裁剪缩小了
            final EventsUnavailableException ex = assertThrows(EventsUnavailableException.class, () -> f.localOnly.fetch(10, 20));
            assertTrue(ex.earliestRecoverableSeq > 20);

            // 接上对象存储：同样的请求现在能补，并且与 leader 当时的事件逐字节一致
            assertSameAsTruth(f.withS3.fetch(10, 20), truth, 10, 20);
            assertEquals("s3-log-start", f.withS3.lastSeed());

            // 本地快照 1 还在：优先用本地的，再用 S3 日志补上被裁剪的那一段
            assertSameAsTruth(f.withS3.fetch(1_300, 1_400), truth, 1_300, 1_400);
            assertTrue(f.withS3.lastSeed().startsWith("snapshot@"), f.withS3.lastSeed());
            assertTrue(f.withS3.lastSeed().contains("lastSeq=" + Fixture.S1_SEQ), f.withS3.lastSeed());

            // 本地快照 1 的录制丢了：降级为对象存储里的快照 1（其日志位置早于本地起点，需 S3 日志 + 本地日志拼接）
            try (io.aeron.archive.client.AeronArchive a = io.aeron.archive.client.AeronArchive.connect(
                    new io.aeron.archive.client.AeronArchive.Context()
                            .aeronDirectoryName(f.node.aeronDirectoryName())
                            .controlRequestChannel("aeron:udp?endpoint=" + f.node.archiveControlEndpoint())
                            .controlResponseChannel("aeron:udp?endpoint=localhost:0"))) {
                final List<Long> ids = f.withS3.localSnapshotRecordingIds(); // 新的在前：[快照2, 快照1]
                a.purgeRecording(ids.get(1));
            }
            assertSameAsTruth(f.withS3.fetch(1_300, 1_400), truth, 1_300, 1_400);
            assertTrue(f.withS3.lastSeed().startsWith("s3-snapshot@"), f.withS3.lastSeed());
            assertTrue(f.withS3.lastSeed().contains("lastSeq=" + Fixture.S1_SEQ), f.withS3.lastSeed());

            // 跨越“对象存储 / 本地”接缝的大区间
            assertSameAsTruth(f.withS3.fetch(1_202, Fixture.TOTAL), truth, 1_202, Fixture.TOTAL);

            // 种子是本地快照 2
            assertSameAsTruth(f.withS3.fetch(Fixture.S2_SEQ + 1, Fixture.TOTAL), truth, Fixture.S2_SEQ + 1, Fixture.TOTAL);
            assertTrue(f.withS3.lastSeed().startsWith("snapshot@"), f.withS3.lastSeed());

            // 整个日志重建出的状态与线上完全一致
            assertEquals(f.node.service().stateHash(), f.withS3.rebuildState().stateHash());
        }
    }

    @Test
    void clusterRestartsFromTrimmedLogAndSequenceContinues() throws Exception {
        assumeTrue(TestMinio.available(), "需要 minio 可执行文件，见 README");
        final File dir;
        final long total = Fixture.TOTAL;
        try (Fixture f = new Fixture()) {
            f.archiver.archiveOnce();
            assertTrue(f.trimmer.trim() > 0);
            dir = f.dir;
            f.client.close();
        } catch (Exception e) {
            throw e;
        }
        final LoggingEventSink sink2 = new LoggingEventSink(false);
        try (ExchangeNode node = ExchangeNode.start(0, 1, dir, false, sink2, TINY);
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            assertTrue(node.service().loadedFromSnapshot());
            c.cancel(1, 2); // 订单 1（序号 2 的那张买单）
            final List<byte[]> r = c.await(1, 15_000);
            assertEquals(EVT_CANCELLED, type(r.get(0)));
            assertEquals(total + 1, seq(r.get(0)));
        }
    }

    @Test
    void trimNeverGoesPastAHoleInTheArchive() throws Exception {
        assumeTrue(TestMinio.available(), "需要 minio 可执行文件，见 README");
        try (Fixture f = new Fixture()) {
            f.archiver.archiveOnce();
            final long rec = f.recordingId();
            final long segLen = 64 * 1024;
            f.store.delete(ArchiveKeys.segment(f.prefix, rec, 2 * segLen)); // 对象存储里丢了第 3 个 segment

            final long newStart = f.trimmer.trim();
            assertEquals(2 * segLen, newStart, "只能裁到连续归档的末端，不能越过缺口");
            assertTrue(new File(f.node.archiveDir(), rec + "-" + (2 * segLen) + ".rec").exists(), "缺口之后的本地 segment 保留");
            assertSameAsTruth(f.withS3.fetch(10, 20), f.truth(), 10, 20);
        }
    }

    @Test
    void trimRefusesWhenArchivedBytesDiffer() throws Exception {
        assumeTrue(TestMinio.available(), "需要 minio 可执行文件，见 README");
        try (Fixture f = new Fixture()) {
            f.archiver.archiveOnce();
            final long rec = f.recordingId();
            final String key = ArchiveKeys.segment(f.prefix, rec, 65_536);
            final byte[] good = f.store.get(key);
            final byte[] bad = good.clone();
            bad[100] ^= 0x5A; // 翻转一个字节，大小不变，并保留原来的 sha256 元数据
            f.store.put(key, bad, java.util.Map.of("sha256", ArchiveKeys.sha256(good)));

            final TrimRefusedException ex = assertThrows(TrimRefusedException.class, f.trimmer::trim);
            assertTrue(ex.getMessage().contains("content differs"), ex.getMessage());
            assertEquals(0, f.withS3.logInfo().startPosition(), "拒绝后本地日志一字节没动");

            // 读取侧同样不会静默使用损坏的对象
            assertThrows(IllegalStateException.class, () -> f.catalog.read(key));
        }
    }

    @Test
    void trimRefusesWhenLatestSnapshotIsNotArchived() throws Exception {
        assumeTrue(TestMinio.available(), "需要 minio 可执行文件，见 README");
        try (Fixture f = new Fixture()) {
            f.archiver.archiveOnce();
            for (ArchiveCatalog.SnapshotInfo s : f.catalog.snapshots()) {
                f.store.delete(s.key());
            }
            final TrimRefusedException ex = assertThrows(TrimRefusedException.class, f.trimmer::trim);
            assertTrue(ex.getMessage().contains("not archived"), ex.getMessage());
        }
    }
}
