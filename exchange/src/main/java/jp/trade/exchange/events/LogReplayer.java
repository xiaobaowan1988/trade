package jp.trade.exchange.events;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import jp.trade.exchange.archive.AeronFrames;
import jp.trade.exchange.archive.ArchiveCatalog;
import jp.trade.exchange.core.MatchingEngine;
import jp.trade.exchange.core.SnapshotFraming;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import static jp.trade.exchange.core.Protocol.CHECKPOINT_LENGTH;
import static jp.trade.exchange.core.Protocol.CHECKPOINT_MAGIC;
import static jp.trade.exchange.core.Protocol.EVENT_LENGTH;
import static jp.trade.exchange.core.Protocol.encodeEvent;

/**
 * 补数进程核心：读取 Raft 日志录制（Aeron Archive），用与线上完全相同的撮合代码做确定性重放，
 * 重新生成任意序号区间的事件。因为状态机是确定性的，重放出的事件与当初 leader 产生的逐字节一致。
 *
 * 从快照开始重放：选“最后事件序号小于请求起点的最新快照”作种子，重放长度不超过一个快照间隔；
 * 没有可用快照时才要求日志从头可读，否则抛 {@link EventsUnavailableException}。
 * 提供 ArchiveCatalog 时，本地已被裁剪的快照与日志改从对象存储读取（见设计 §4.3）。
 * 骨架限制：每次补数都重新读取并重放（未缓存引擎状态）。
 */
public final class LogReplayer implements GapFillingConsumer.Backfill, AutoCloseable {
    private final File clusterDir;
    private final Aeron aeron;
    private final AeronArchive archive;
    private volatile String lastSeed = "none";
    private volatile long checkpointsVerified;
    private final ArchiveCatalog catalog; // 可为 null：只用本地日志

    /**
     * @param clusterDir      某个集群成员的 cluster 目录（含 recording.log），或其拷贝
     * @param aeronDir        本进程要连接的 Aeron MediaDriver 目录
     * @param archiveEndpoint 该成员 Archive 的控制端口，形如 host:port
     */
    public LogReplayer(File clusterDir, String aeronDir, String archiveEndpoint) {
        this(clusterDir, aeronDir, archiveEndpoint, null);
    }

    /** @param catalog 对象存储里的归档；非 null 时，本地已裁剪的部分改从归档读取 */
    public LogReplayer(File clusterDir, String aeronDir, String archiveEndpoint, ArchiveCatalog catalog) {
        this.catalog = catalog;
        this.clusterDir = clusterDir;
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        this.archive = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .ownsAeronClient(false)
                .controlRequestChannel("aeron:udp?endpoint=" + archiveEndpoint)
                .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
    }

    /** 重放已提交日志，把序号在 [fromSeq, toSeq]（含）内的事件交给 out。返回重放到的最后一个事件序号。 */
    public long replay(long fromSeq, long toSeq, jp.trade.exchange.core.EventHandler out) {
        return run(fromSeq, toSeq, out).lastSeq;
    }

    /** 重放整个已提交日志，返回日志头处的完整撮合状态（用于与投影对账）。 */
    public MatchingEngine rebuildState() {
        return run(Long.MAX_VALUE, Long.MAX_VALUE, (a, b, c, d, e, f, g, h, i, j) -> {}).engine;
    }

    private record Result(MatchingEngine engine, long lastSeq) {}

    private Result run(long fromSeq, long toSeq, jp.trade.exchange.core.EventHandler out) {
        final RecordingLog.RecoveryPlan plan;
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            plan = log.createRecoveryPlan(archive, 1, io.aeron.Aeron.NULL_VALUE);
        }
        if (plan.log() == null) {
            return new Result(new MatchingEngine(), 0); // 还没有任何日志
        }
        final long recordingId = plan.log().recordingId();
        final long localStart = archive.getStartPosition(recordingId);
        final long end = committedEnd(plan, recordingId);

        // 对象存储能否接上本地日志：归档必须从更早处开始，并连续覆盖到本地起点
        final ArchiveCatalog.Coverage cov = catalog == null ? null : catalog.coverage(recordingId);
        final boolean archiveBridges = cov != null && cov.startPosition() < localStart && cov.contiguousEnd() >= localStart;
        final long effectiveStart = archiveBridges ? cov.startPosition() : localStart;

        // ---- 种子：本地 + 归档里的全部快照，选最后事件序号 < fromSeq 的最新者 ----
        record Seed(long pos, long lastSeq, String origin, java.util.function.Supplier<MatchingEngine> loader) {}
        final List<Seed> seeds = new ArrayList<>();
        final java.util.Set<Long> localPositions = new java.util.HashSet<>();
        for (SnapshotRef snap : serviceSnapshotsNewestFirst()) {
            if (snap.logPosition < effectiveStart || snap.logPosition > end) {
                continue; // 早于可读日志的起点，或晚于已提交位置：无法与日志衔接
            }
            final byte[] state;
            try {
                state = readSnapshotBytes(snap.recordingId);
            } catch (io.aeron.archive.client.ArchiveException e) {
                continue; // 本地快照录制丢了（换盘/误删）：降级，改用对象存储里的副本
            }
            final MatchingEngine eng = MatchingEngine.fromSnapshot(state);
            localPositions.add(snap.logPosition);
            seeds.add(new Seed(snap.logPosition, eng.lastEventSeq(), "snapshot", () -> eng));
        }
        if (catalog != null) {
            for (ArchiveCatalog.SnapshotInfo si : catalog.snapshots()) {
                if (si.logPosition() < effectiveStart || si.logPosition() > end || localPositions.contains(si.logPosition())) {
                    continue;
                }
                seeds.add(new Seed(si.logPosition(), si.lastSeq(), "s3-snapshot",
                        () -> MatchingEngine.fromSnapshot(catalog.read(si.key()))));
            }
        }
        seeds.sort((x, y) -> Long.compare(y.pos, x.pos));

        MatchingEngine engine = null;
        long startPos = effectiveStart;
        long oldestSeedLastSeq = Long.MAX_VALUE;
        for (Seed seed : seeds) {
            oldestSeedLastSeq = Math.min(oldestSeedLastSeq, seed.lastSeq);
            if (engine == null && seed.lastSeq < fromSeq) {
                engine = seed.loader.get();
                startPos = seed.pos;
                lastSeed = seed.origin + "@" + seed.pos + " lastSeq=" + seed.lastSeq;
            }
        }
        if (engine == null) {
            if (effectiveStart != 0) {
                throw new EventsUnavailableException(fromSeq,
                        oldestSeedLastSeq == Long.MAX_VALUE ? Long.MAX_VALUE : oldestSeedLastSeq + 1);
            }
            engine = new MatchingEngine();
            startPos = 0;
            lastSeed = archiveBridges ? "s3-log-start" : "log-start";
        }
        if (end <= startPos) {
            return new Result(engine, engine.lastEventSeq());
        }
        final MatchingEngine seeded = engine;

        final long[] last = {engine.lastEventSeq()};
        final boolean[] done = {false};
        final jp.trade.exchange.core.EventHandler filter = (type, aux, seq, ts, orderId, account, price, qty, co, ca) -> {
            last[0] = seq;
            if (seq >= fromSeq && seq <= toSeq) {
                out.onEvent(type, aux, seq, ts, orderId, account, price, qty, co, ca);
            }
            if (seq >= toSeq) {
                done[0] = true;
            }
        };

        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final SessionMessageHeaderDecoder session = new SessionMessageHeaderDecoder();
        final java.util.TreeMap<Long, Long> pendingChecks = new java.util.TreeMap<>();
        final long[] checkpointOut = new long[2];
        // 对象存储与本地 Archive 两条读取路径共用同一个“日志消息 → 命令”的处理
        final AeronFrames.MessageHandler onMessage = (DirectBuffer buf, int offset, int length, long position) -> {
            header.wrap(buf, offset);
            if (header.templateId() != SessionMessageHeaderDecoder.TEMPLATE_ID) {
                return; // 会话开关、选主、定时器等集群内部消息，不是业务命令
            }
            session.wrap(buf, offset + MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
            final int payloadOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH + header.blockLength();
            final int payloadLength = length - MessageHeaderDecoder.ENCODED_LENGTH - header.blockLength();
            // 服务自己发的检查点（服务会话号为负；客户端会话号为正，伪造不了）：不是命令，不能交给引擎，只用来核对
            if (session.clusterSessionId() < 0 && payloadLength == CHECKPOINT_LENGTH && buf.getInt(payloadOffset) == CHECKPOINT_MAGIC) {
                final long seq = buf.getLong(payloadOffset + 4);
                final long recorded = buf.getLong(payloadOffset + 12);
                final Long mine = pendingChecks.remove(seq);
                if (mine == null) {
                    return; // 例如检查点在快照之前发出、之后才到：无法核对
                }
                if (mine != recorded) {
                    throw new IllegalStateException("log replay integrity failure: checkpoint mismatch at event seq " + seq
                            + " (replayed " + mine + ", log recorded " + recorded + ")");
                }
                checkpointsVerified++;
                return;
            }
            // 时间戳取日志里记录的集群时间，与线上服务收到的 timestamp 一致
            seeded.onCommand(buf, payloadOffset, payloadLength, session.timestamp(), filter);
            if (seeded.takeCheckpoint(checkpointOut)) { // 与线上服务在同一位置推进检查点边界，保持状态一致
                pendingChecks.put(checkpointOut[0], checkpointOut[1]);
            }
        };

        // ---- A. 本地已裁剪的部分：从对象存储读 [startPos, localStart) ----
        if (startPos < localStart) {
            final AeronFrames.Reassembler re = new AeronFrames.Reassembler();
            catalog.readMessages(recordingId, startPos, Math.min(localStart, end), re, onMessage, () -> done[0]);
            if (re.hasPartialMessage() && !done[0]) {
                throw new IllegalStateException("log message fragmented across the archive/local seam");
            }
        }
        if (done[0]) {
            return new Result(seeded, last[0]);
        }

        // ---- B. 本地 Archive：[max(startPos, localStart), end) ----
        final long localFrom = Math.max(startPos, localStart);
        if (end <= localFrom) {
            return new Result(seeded, last[0]);
        }
        final FragmentHandler handler = new io.aeron.FragmentAssembler((DirectBuffer buf, int offset, int length, Header h) ->
                onMessage.onMessage(buf, offset, length, h.position()));
        final int replayStream = 4001;
        final Subscription sub = openReplay(recordingId, localFrom, end - localFrom, replayStream); // 重放发布端不接受端口 0，需显式端口
        try {
            final IdleStrategy idle = new BackoffIdleStrategy(10, 10, 1_000, 1_000_000);
            final long deadline = System.currentTimeMillis() + 60_000;
            Image image = null;
            while (!done[0]) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("log replay timeout");
                }
                if (image == null) {
                    image = sub.imageCount() > 0 ? sub.imageAtIndex(0) : null;
                }
                final int n = sub.poll(handler, 100);
                if (n == 0) {
                    if (image != null && (image.isEndOfStream() || image.isClosed() || image.position() >= end)) {
                        break;
                    }
                    idle.idle();
                }
            }
        } finally {
            archive.stopAllReplays(recordingId);
            CloseHelper.quietClose(sub);
        }
        return new Result(seeded, last[0]);
    }

    /**
     * 重放终点必须是“已提交”位置，不能用录制末端（leader 的录制里可能含尚未提交的尾部）。
     * term 已结束：用 RecordingLog 中记录的提交位置；term 仍在进行：RecordingLog 里还是 0，
     * 要读本节点 ConsensusModule 的 commit-position 计数器。
     */
    private record SnapshotRef(long recordingId, long logPosition) {}

    /**
     * 全部有效的撮合服务快照，新的在前。注意不能用 RecoveryPlan.snapshots()：它只含“最新一份”，
     * 而补数需要在更早的快照里挑一个序号足够小的做种子。
     */
    private List<SnapshotRef> serviceSnapshotsNewestFirst() {
        final List<SnapshotRef> l = new ArrayList<>();
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            for (RecordingLog.Entry e : log.entries()) {
                // serviceId 0 是撮合服务；-1 是 ConsensusModule 自己的快照
                if (e.isValid && e.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && e.serviceId == 0) {
                    l.add(new SnapshotRef(e.recordingId, e.logPosition));
                }
            }
        }
        l.sort((x, y) -> Long.compare(y.logPosition, x.logPosition));
        return l;
    }

    /** 从 Archive 读取一份服务快照的状态字节。 */
    private byte[] readSnapshotBytes(long snapshotRecordingId) {
        final long from = archive.getStartPosition(snapshotRecordingId);
        final long stop = archive.getStopPosition(snapshotRecordingId);
        final SnapshotFraming.Reader reader = new SnapshotFraming.Reader();
        final FragmentHandler h = new io.aeron.FragmentAssembler((buf, offset, length, header) -> reader.accept(buf, offset, length));
        final Subscription sub = openReplay(snapshotRecordingId, from, stop - from, 4002);
        try {
            final IdleStrategy idle = new BackoffIdleStrategy(10, 10, 1_000, 1_000_000);
            final long deadline = System.currentTimeMillis() + 30_000;
            while (!reader.complete()) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("snapshot read timeout");
                }
                final int n = sub.poll(h, 50);
                if (n == 0) {
                    if (sub.imageCount() > 0 && (sub.imageAtIndex(0).isEndOfStream() || sub.imageAtIndex(0).isClosed())) {
                        break;
                    }
                    idle.idle();
                }
            }
        } finally {
            archive.stopAllReplays(snapshotRecordingId);
            CloseHelper.quietClose(sub);
        }
        return reader.bytes();
    }

    /** 重放过程中累计核对通过的链式哈希检查点数（日志完整性的独立证据）。 */
    public long checkpointsVerified() { return checkpointsVerified; }

    /** 最近一次补数用的种子（测试/运维可见）：log-start 或 snapshot@位置。 */
    public String lastSeed() { return lastSeed; }

    /** 集群里已有的服务快照数量。 */
    public int snapshotCount() {
        return serviceSnapshotsNewestFirst().size();
    }

    /** 日志录制的基本信息，供归档/裁剪使用。 */
    public record LogInfo(long recordingId, long startPosition, long committedEnd, int segmentFileLength) {}

    /** 本地快照内容，供归档使用。 */
    public record SnapshotBlob(long logPosition, long lastSeq, byte[] state) {}

    /** 还没有日志时返回 null。 */
    public LogInfo logInfo() {
        final RecordingLog.RecoveryPlan plan;
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            plan = log.createRecoveryPlan(archive, 1, io.aeron.Aeron.NULL_VALUE);
        }
        if (plan.log() == null) {
            return null;
        }
        final long recordingId = plan.log().recordingId();
        final int[] segmentLength = {0};
        archive.listRecording(recordingId, (controlSessionId, correlationId, recId, startTimestamp, stopTimestamp,
                                             startPosition, stopPosition, initialTermId, segLen, termBufferLength,
                                             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity)
                -> segmentLength[0] = segLen);
        return new LogInfo(recordingId, archive.getStartPosition(recordingId), committedEnd(plan, recordingId), segmentLength[0]);
    }

    /** 本地撮合服务快照所在的 Archive 录制编号，新的在前（运维/测试用）。 */
    public List<Long> localSnapshotRecordingIds() {
        final List<Long> ids = new ArrayList<>();
        for (SnapshotRef snap : serviceSnapshotsNewestFirst()) {
            ids.add(snap.recordingId);
        }
        return ids;
    }

    public List<SnapshotBlob> localSnapshotBlobs() {
        final List<SnapshotBlob> l = new ArrayList<>();
        for (SnapshotRef snap : serviceSnapshotsNewestFirst()) {
            final byte[] state;
            try {
                state = readSnapshotBytes(snap.recordingId);
            } catch (io.aeron.archive.client.ArchiveException e) {
                continue;
            }
            l.add(new SnapshotBlob(snap.logPosition, MatchingEngine.lastEventSeqOfSnapshot(state), state));
        }
        return l;
    }

    /**
     * 删除日志里 position 之前的整段 segment。**不做任何归档检查**，只允许 {@code LogTrimmer} 在验证完
     * 对象存储中的副本之后调用。返回裁剪后的日志起点。
     */
    public long purgeSegmentsUnsafe(long recordingId, long position) {
        archive.purgeSegments(recordingId, position);
        return archive.getStartPosition(recordingId);
    }

    /**
     * 开一个 Archive 重放订阅。端口先探测、后绑定，探测与绑定之间可能被别的 socket 抢走
     * （Address already in use），所以换端口重试几次。
     */
    private Subscription openReplay(long recordingId, long position, long length, int streamId) {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 8; attempt++) {
            try {
                return archive.replay(recordingId, position, length, "aeron:udp?endpoint=localhost:" + freeUdpPort(), streamId);
            } catch (io.aeron.exceptions.RegistrationException e) {
                if (e.getMessage() == null || !e.getMessage().contains("Address already in use")) {
                    throw e;
                }
                last = e;
            }
        }
        throw last;
    }

    private static int freeUdpPort() {
        try (java.net.DatagramSocket s = new java.net.DatagramSocket(0)) {
            return s.getLocalPort();
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private long committedEnd(RecordingLog.RecoveryPlan plan, long recordingId) {
        final long live = liveCommitPosition();
        if (live >= 0) {
            return Math.min(live, archive.getMaxRecordedPosition(recordingId));
        }
        return plan.committedLogPosition();
    }

    private long liveCommitPosition() {
        final long[] v = {-1};
        final org.agrona.concurrent.status.CountersReader counters = aeron.countersReader();
        counters.forEach((counterId, typeId, keyBuffer, label) -> {
            if (typeId == io.aeron.cluster.ConsensusModule.Configuration.COMMIT_POSITION_TYPE_ID) {
                v[0] = counters.getCounterValue(counterId);
            }
        });
        return v[0];
    }

    @Override
    public List<byte[]> fetch(long fromSeq, long toSeq) {
        final List<byte[]> events = new ArrayList<>();
        final ExpandableArrayBuffer b = new ExpandableArrayBuffer(EVENT_LENGTH);
        replay(fromSeq, toSeq, (type, aux, seq, ts, orderId, account, price, qty, co, ca) -> {
            encodeEvent(b, 0, type, aux, seq, ts, orderId, account, price, qty, co, ca);
            final byte[] e = new byte[EVENT_LENGTH];
            b.getBytes(0, e);
            events.add(e);
        });
        return events;
    }

    @Override
    public long headSeq() {
        return replay(Long.MAX_VALUE, Long.MAX_VALUE, (a, b, c, d, e, f, g, h, i, j) -> {});
    }

    @Override
    public void close() {
        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(aeron);
    }
}
