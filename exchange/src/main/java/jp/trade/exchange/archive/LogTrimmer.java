package jp.trade.exchange.archive;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import jp.trade.exchange.events.LogReplayer;

/**
 * 受保护的日志裁剪：只有在对象存储里确认存在**逐字节一致**的副本之后才会删除本地 segment。
 * 这是代码里裁剪日志的唯一入口（LogReplayer 只暴露 purgeSegmentsUnsafe 给它）。
 *
 * 规则见设计 §4.3：目标 = min(最新快照所在 segment 边界, 对象存储连续归档末端)；
 * 最新快照本身也必须已归档；任何一项缺失或不符都拒绝并说明原因。
 */
public final class LogTrimmer {
    private final LogReplayer replayer;
    private final ArchiveCatalog catalog;
    private final File archiveDir;

    public LogTrimmer(LogReplayer replayer, ArchiveCatalog catalog, File archiveDir) {
        this.replayer = replayer;
        this.catalog = catalog;
        this.archiveDir = archiveDir;
    }

    /** @return 裁剪后的本地日志起点（没有可裁剪的内容则为当前起点） */
    public long trim() {
        final LogReplayer.LogInfo info = replayer.logInfo();
        if (info == null) {
            throw new TrimRefusedException("no log recording");
        }
        final ObjectStore store = catalog.store();
        final long rec = info.recordingId();
        final long segLen = info.segmentFileLength();

        final List<LogReplayer.SnapshotBlob> blobs = replayer.localSnapshotBlobs();
        if (blobs.isEmpty()) {
            throw new TrimRefusedException("no snapshot exists; nothing is safe to trim to");
        }
        final LogReplayer.SnapshotBlob latest = blobs.stream().max(Comparator.comparingLong(LogReplayer.SnapshotBlob::logPosition)).get();

        // 最新快照必须已归档且内容一致
        final String snapKey = ArchiveKeys.snapshot(catalog.prefix(), latest.logPosition(), latest.lastSeq());
        final ObjectStore.Stat snapStat = store.stat(snapKey);
        if (snapStat == null) {
            throw new TrimRefusedException("latest snapshot @" + latest.logPosition() + " is not archived");
        }
        if (!ArchiveKeys.sha256(latest.state()).equals(ArchiveKeys.sha256(store.get(snapKey)))) {
            throw new TrimRefusedException("archived snapshot @" + latest.logPosition() + " differs from local");
        }

        final ArchiveCatalog.Coverage cov = catalog.coverage(rec);
        if (cov == null) {
            throw new TrimRefusedException("nothing archived for recording " + rec);
        }
        final long target = Math.min(latest.logPosition() / segLen * segLen, cov.contiguousEnd());
        if (target <= info.startPosition()) {
            return info.startPosition();
        }

        // 逐个 segment 核对：存在，且内容与本地文件完全一致（重新下载并比对哈希，不信任元数据）
        for (long base = info.startPosition() / segLen * segLen; base < target; base += segLen) {
            final String key = ArchiveKeys.segment(catalog.prefix(), rec, base);
            final File local = new File(archiveDir, rec + "-" + base + ".rec");
            final ObjectStore.Stat st = store.stat(key);
            if (st == null) {
                throw new TrimRefusedException("segment " + base + " is not in the archive");
            }
            if (local.exists()) {
                if (st.size() != local.length()) {
                    throw new TrimRefusedException("segment " + base + " size differs: archive=" + st.size() + " local=" + local.length());
                }
                if (!ArchiveKeys.sha256(local).equals(ArchiveKeys.sha256(store.get(key)))) {
                    throw new TrimRefusedException("segment " + base + " content differs from the local file");
                }
            }
        }
        return replayer.purgeSegmentsUnsafe(rec, target);
    }
}
