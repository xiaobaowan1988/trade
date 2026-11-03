package jp.trade.exchange.archive;

import java.io.File;
import java.util.Map;
import jp.trade.exchange.events.LogReplayer;

/**
 * 把本地 Archive 里已完成的日志 segment 与撮合快照归档到对象存储（S3 / MinIO）。
 * 可随时重复调用、随时崩溃重启：已存在且大小与 SHA-256 一致的对象会被跳过。
 *
 * 需要读取成员的 Archive 目录，所以与节点部署在同一台机器上（sidecar）。
 */
public final class LogArchiver {
    public record Report(int segmentsUploaded, int snapshotsUploaded, long archivedEnd) {}

    private final LogReplayer replayer;
    private final ArchiveCatalog catalog;
    private final File archiveDir;

    public LogArchiver(LogReplayer replayer, ArchiveCatalog catalog, File archiveDir) {
        this.replayer = replayer;
        this.catalog = catalog;
        this.archiveDir = archiveDir;
    }

    public Report archiveOnce() {
        final LogReplayer.LogInfo info = replayer.logInfo();
        if (info == null) {
            return new Report(0, 0, 0);
        }
        final ObjectStore store = catalog.store();
        final String prefix = catalog.prefix();
        final long rec = info.recordingId();
        final int segLen = info.segmentFileLength();

        // 录制元数据：首次写入；之后必须一致
        final String metaKey = ArchiveKeys.meta(prefix, rec);
        if (store.stat(metaKey) == null) {
            store.put(metaKey, ArchiveKeys.json(info.startPosition(), segLen), Map.of());
        } else {
            final long[] m = ArchiveKeys.parseJson(store.get(metaKey));
            if (m[1] != segLen || m[0] > info.startPosition()) {
                throw new IllegalStateException("archive meta conflicts with local recording: " + java.util.Arrays.toString(m));
            }
        }

        int segments = 0;
        // 只归档完全落在已提交位置之内的 segment；活动 segment 不归档
        for (long base = Math.min(info.startPosition(), catalog.coverage(rec).startPosition()) / segLen * segLen;
             base + segLen <= info.committedEnd(); base += segLen) {
            final String key = ArchiveKeys.segment(prefix, rec, base);
            final File f = new File(archiveDir, rec + "-" + base + ".rec");
            if (!f.exists()) {
                // 本地已被裁剪：此前必须已归档（LogTrimmer 保证），这里再核对一次
                if (store.stat(key) == null) {
                    throw new IllegalStateException("segment " + base + " is gone locally and absent from archive");
                }
                continue;
            }
            final String sha = ArchiveKeys.sha256(f);
            final ObjectStore.Stat st = store.stat(key);
            if (st != null && st.size() == f.length() && sha.equals(st.metadata().get("sha256"))) {
                continue; // 已归档
            }
            store.put(key, f, Map.of("sha256", sha));
            verifyUploaded(store, key, f.length(), sha);
            segments++;
        }

        int snapshots = 0;
        for (LogReplayer.SnapshotBlob blob : replayer.localSnapshotBlobs()) {
            final String key = ArchiveKeys.snapshot(prefix, blob.logPosition(), blob.lastSeq());
            final String sha = ArchiveKeys.sha256(blob.state());
            final ObjectStore.Stat st = store.stat(key);
            if (st != null && st.size() == blob.state().length && sha.equals(st.metadata().get("sha256"))) {
                continue;
            }
            store.put(key, blob.state(), Map.of("sha256", sha));
            verifyUploaded(store, key, blob.state().length, sha);
            snapshots++;
        }
        final ArchiveCatalog.Coverage cov = catalog.coverage(rec);
        return new Report(segments, snapshots, cov == null ? 0 : cov.contiguousEnd());
    }

    private static void verifyUploaded(ObjectStore store, String key, long size, String sha) {
        final ObjectStore.Stat st = store.stat(key);
        if (st == null || st.size() != size || !sha.equals(st.metadata().get("sha256"))) {
            throw new IllegalStateException("upload verification failed for " + key);
        }
    }
}
