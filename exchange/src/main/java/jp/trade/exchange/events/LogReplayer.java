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
 * 骨架限制：每次都从日志起点重放（O(日志长度)）；日志起点必须为 0（未裁剪）。
 * 生产形态应从快照种子化引擎，并缓存引擎状态或读取 S3 上的日志归档。
 */
public final class LogReplayer implements GapFillingConsumer.Backfill, AutoCloseable {
    private final File clusterDir;
    private final Aeron aeron;
    private final AeronArchive archive;

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
        final RecordingLog.RecoveryPlan plan;
        try (RecordingLog log = new RecordingLog(clusterDir, false)) {
            plan = log.createRecoveryPlan(archive, 1, io.aeron.Aeron.NULL_VALUE);
        }
        if (plan.log() == null) {
            return 0; // 还没有任何日志
        }
        final long recordingId = plan.log().recordingId();
        final long start = archive.getStartPosition(recordingId);
        if (start != 0) {
            throw new UnsupportedOperationException("日志已被裁剪(start=" + start + ")，需要从快照种子化引擎");
        }
        final long end = committedEnd(plan, recordingId);
        if (end <= start) {
            return 0;
        }

        final MatchingEngine engine = new MatchingEngine();
        final long[] last = {0};
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
            engine.onCommand(buf, payloadOffset, payloadLength, session.timestamp(), filter);
        });

        final int replayStream = 4001;
        final String replayChannel = "aeron:udp?endpoint=localhost:" + freeUdpPort(); // 重放发布端不接受端口 0
        final Subscription sub = archive.replay(recordingId, start, end - start, replayChannel, replayStream);
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
        return last[0];
    }

    /**
     * 重放终点必须是“已提交”位置，不能用录制末端（leader 的录制里可能含尚未提交的尾部）。
     * term 已结束：用 RecordingLog 中记录的提交位置；term 仍在进行：RecordingLog 里还是 0，
     * 要读本节点 ConsensusModule 的 commit-position 计数器。
     */
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
