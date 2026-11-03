package jp.trade.exchange.archive;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

/** 读取对象存储里的归档：覆盖范围、快照清单、带校验的 segment/快照读取，以及按位置解析日志消息。 */
public final class ArchiveCatalog {
    private final ObjectStore store;
    private final String prefix;

    public ArchiveCatalog(ObjectStore store, String prefix) {
        this.store = store;
        this.prefix = prefix;
    }

    public ObjectStore store() { return store; }

    public String prefix() { return prefix; }

    /** @param contiguousEnd 从首个 segment 起连续归档到的末端位置；等于首个 segment 起点表示一个都没有 */
    public record Coverage(long startPosition, long segmentFileLength, long contiguousEnd) {}

    public record SnapshotInfo(String key, long logPosition, long lastSeq) {}

    /** 该录制在对象存储里的覆盖情况；没有 meta.json 则返回 null。 */
    public Coverage coverage(long recordingId) {
        final String metaKey = ArchiveKeys.meta(prefix, recordingId);
        if (store.stat(metaKey) == null) {
            return null;
        }
        final long[] meta = ArchiveKeys.parseJson(store.get(metaKey));
        final long start = meta[0];
        final long segLen = meta[1];
        final Set<Long> bases = new HashSet<>();
        for (String k : store.list(ArchiveKeys.segmentPrefix(prefix, recordingId))) {
            final long b = ArchiveKeys.parseSegmentBase(k);
            if (b >= 0) {
                bases.add(b);
            }
        }
        long expected = start / segLen * segLen;
        while (bases.contains(expected)) {
            expected += segLen;
        }
        return new Coverage(start, segLen, expected);
    }

    /** 全部已归档快照，新的在前（序号信息来自键名，无需下载）。 */
    public List<SnapshotInfo> snapshots() {
        final List<SnapshotInfo> l = new ArrayList<>();
        for (String k : store.list(ArchiveKeys.snapshotPrefix(prefix))) {
            final long[] p = ArchiveKeys.parseSnapshot(k);
            if (p != null) {
                l.add(new SnapshotInfo(k, p[0], p[1]));
            }
        }
        l.sort((a, b) -> Long.compare(b.logPosition, a.logPosition));
        return l;
    }

    /** 下载并校验 SHA-256；与元数据不符直接报错，绝不静默使用。 */
    public byte[] read(String key) {
        final ObjectStore.Stat st = store.stat(key);
        if (st == null) {
            throw new IllegalStateException("archive object missing: " + key);
        }
        final byte[] data = store.get(key);
        final String expected = st.metadata().get("sha256");
        if (expected == null || !expected.equals(ArchiveKeys.sha256(data))) {
            throw new IllegalStateException("archive object corrupt (sha256 mismatch): " + key);
        }
        return data;
    }

    /**
     * 按日志位置 [fromPos, toPos) 读取归档里的日志消息（Aeron 帧解析 + 分片重组）。
     * 调用方负责保证 [fromPos, toPos) 在 coverage 之内，且 fromPos 在帧边界上。
     */
    public void readMessages(long recordingId, long fromPos, long toPos, AeronFrames.Reassembler re,
                             AeronFrames.MessageHandler handler, BooleanSupplier stop) {
        final Coverage cov = coverage(recordingId);
        if (cov == null || fromPos < cov.startPosition() || toPos > cov.contiguousEnd()) {
            throw new IllegalStateException("range [" + fromPos + "," + toPos + ") not covered by archive " + cov);
        }
        final long segLen = cov.segmentFileLength();
        for (long base = fromPos / segLen * segLen; base < toPos && !stop.getAsBoolean(); base += segLen) {
            final byte[] data = read(ArchiveKeys.segment(prefix, recordingId, base));
            final long fileStart = Math.max(fromPos - base, Math.max(cov.startPosition() - base, 0));
            AeronFrames.parse(data, (int) fileStart, base, toPos, re, handler);
        }
    }
}
