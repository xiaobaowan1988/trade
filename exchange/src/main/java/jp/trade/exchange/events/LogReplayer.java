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
import jp.trade.exchange.core.MatchingEngine;
import jp.trade.exchange.core.SnapshotFraming;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import static jp.trade.exchange.core.Protocol.EVENT_LENGTH;
import static jp.trade.exchange.core.Protocol.encodeEvent;

/**
 * 补数进程核心：读取 Raft 日志录制（Aeron Archive），用与线上完全相同的撮合代码做确定性重放，
 * 重新生成任意序号区间的事件。因为状态机是确定性的，重放出的事件与当初 leader 产生的逐字节一致。
 *
 * 从快照开始重放：选“最后事件序号小于请求起点的最新快照”作种子，重放长度不超过一个快照间隔；
 * 没有可用快照时才要求日志从头可读，否则抛 {@link EventsUnavailableException}。
 * 骨架限制：每次补数都重新读取并重放（未缓存引擎状态）；尚未对接 S3 归档。
 */
public final class LogReplayer implements GapFillingConsumer.Backfill, AutoCloseable {
    private final File clusterDir;
    private final Aeron aeron;
    private final AeronArchive archive;
    private volatile String lastSeed = "none";

    /**
     * @param clusterDir      某个集群成员的 cluster 目录（含 recording.log），或其拷贝
     * @param aeronDir        本进程要连接的 Aeron MediaDriver 目录
     * @param archiveEndpoint 该成员 Archive 的控制端口，形如 host:port
     */
    public LogReplayer(File clusterDir, String aeronDir, String archiveEndpoint) {
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
        final long start = archive.getStartPosition(recordingId);
        final long end = committedEnd(plan, recordingId);

        // ---- 选种子：最后事件序号 < fromSeq 的最新快照；没有则要求日志从头可读 ----
        MatchingEngine engine = null;
        long startPos = start;
        long oldestUsableSnapshotLastSeq = Long.MAX_VALUE;
        for (SnapshotRef snap : serviceSnapshotsNewestFirst()) {
            if (snap.logPosition < start || snap.logPosition > end) {
                continue; // 快照早于被裁剪的日志起点，或晚于已提交位置：不能与日志衔接
            }
            final MatchingEngine candidate = readSnapshot(snap.recordingId);
            oldestUsableSnapshotLastSeq = Math.min(oldestUsableSnapshotLastSeq, candidate.lastEventSeq());
            if (engine == null && candidate.lastEventSeq() < fromSeq) {
                engine = candidate;
                startPos = snap.logPosition;
                lastSeed = "snapshot@" + snap.logPosition + " lastSeq=" + candidate.lastEventSeq();
            }
        }
        if (engine == null) {
            if (start != 0) {
                final long earliest = oldestUsableSnapshotLastSeq == Long.MAX_VALUE
                        ? Long.MAX_VALUE : oldestUsableSnapshotLastSeq + 1;
                throw new EventsUnavailableException(fromSeq, earliest);
            }
            engine = new MatchingEngine();
            startPos = 0;
            lastSeed = "log-start";
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
        final FragmentHandler handler = new io.aeron.FragmentAssembler((DirectBuffer buf, int offset, int length, Header h) -> {
            header.wrap(buf, offset);
            if (header.templateId() != SessionMessageHeaderDecoder.TEMPLATE_ID) {
                return; // 会话开关、选主、定时器等集群内部消息，不是业务命令
            }
            session.wrap(buf, offset + MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
            final int payloadOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH + header.blockLength();
            final int payloadLength = length - MessageHeaderDecoder.ENCODED_LENGTH - header.blockLength();
            // 时间戳取日志里记录的集群时间，与线上服务收到的 timestamp 一致
            seeded.onCommand(buf, payloadOffset, payloadLength, session.timestamp(), filter);
        });

        final int replayStream = 4001;
        final String replayChannel = "aeron:udp?endpoint=localhost:" + freeUdpPort(); // 重放发布端不接受端口 0
        final Subscription sub = archive.replay(recordingId, startPos, end - startPos, replayChannel, replayStream);
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

    /** 从 Archive 读取一份服务快照并还原成引擎。 */
    private MatchingEngine readSnapshot(long snapshotRecordingId) {
        final long from = archive.getStartPosition(snapshotRecordingId);
        final long stop = archive.getStopPosition(snapshotRecordingId);
        final SnapshotFraming.Reader reader = new SnapshotFraming.Reader();
        final FragmentHandler h = new io.aeron.FragmentAssembler((buf, offset, length, header) -> reader.accept(buf, offset, length));
        final Subscription sub = archive.replay(snapshotRecordingId, from, stop - from,
                "aeron:udp?endpoint=localhost:" + freeUdpPort(), 4002);
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
        return MatchingEngine.fromSnapshot(reader.bytes());
    }

    /** 最近一次补数用的种子（测试/运维可见）：log-start 或 snapshot@位置。 */
    public String lastSeed() { return lastSeed; }

    /** 集群里已有的服务快照数量。 */
    public int snapshotCount() {
        return serviceSnapshotsNewestFirst().size();
    }

    /**
     * 把日志裁剪到最新快照所在的 segment 边界（只删整个 segment）。调用前必须已把这些日志归档到 S3，
     * 否则早于本地最早可恢复序号的缺口再也补不回来。返回裁剪后的日志起点。
     */
    public long trimLogToLatestSnapshot() {
        final RecordingLog.RecoveryPlan plan;
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            plan = log.createRecoveryPlan(archive, 1, io.aeron.Aeron.NULL_VALUE);
        }
        final List<SnapshotRef> snaps = serviceSnapshotsNewestFirst();
        if (snaps.isEmpty()) {
            throw new IllegalStateException("no snapshot to trim to");
        }
        final long recordingId = plan.log().recordingId();
        final int[] segmentLength = {0};
        archive.listRecording(recordingId, (controlSessionId, correlationId, recId, startTimestamp, stopTimestamp,
                                             startPosition, stopPosition, initialTermId, segLen, termBufferLength,
                                             mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity)
                -> segmentLength[0] = segLen);
        final long target = snaps.get(0).logPosition / segmentLength[0] * segmentLength[0];
        if (target > archive.getStartPosition(recordingId)) {
            archive.purgeSegments(recordingId, target);
        }
        return archive.getStartPosition(recordingId);
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
