package jp.trade.exchange.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import jp.trade.exchange.core.MatchingEngine;
import jp.trade.exchange.core.SnapshotFraming;
import jp.trade.exchange.events.EventBridge;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import static jp.trade.exchange.core.Protocol.*;

/**
 * 集群服务：Raft 提交后的日志在此被回放进撮合状态机（定序与撮合在同一集群内完成）。
 *
 * - 业务线程只做：解码 → 引擎 → 把事件 offer 到 IPC 流（leader 才发）→ 把确认写回请求会话。
 * - 事件去向 Kafka 由 {@link EventBridge} 的独立线程负责。
 */
public final class ExchangeService implements ClusteredService {
    private Cluster cluster;
    private MatchingEngine engine = new MatchingEngine();
    private Publication eventPublication;

    private final ExpandableArrayBuffer eventBuf = new ExpandableArrayBuffer(EVENT_LENGTH);
    private final UnsafeBuffer eventView = new UnsafeBuffer(eventBuf);

    // 当前命令的上下文（在单线程内使用，避免 lambda 分配）
    private ClientSession currentSession;
    private boolean leader;
    private long droppedEvents;
    private volatile boolean loadedFromSnapshot;
    private volatile Cluster.Role role = Cluster.Role.FOLLOWER;

    private final jp.trade.exchange.core.EventHandler handler = this::onEngineEvent;

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        this.eventPublication = cluster.aeron().addPublication(EventBridge.CHANNEL, EventBridge.STREAM_ID);
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
            loadedFromSnapshot = true;
        }
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {}

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {}

    @Override
    public void onSessionMessage(ClientSession session, long timestamp, DirectBuffer buffer, int offset, int length, Header header) {
        leader = cluster.role() == Cluster.Role.LEADER;
        currentSession = leader ? session : null;
        engine.onCommand(buffer, offset, length, timestamp, handler);
        currentSession = null;
    }

    private void onEngineEvent(int type, int aux, long seq, long ts, long orderId, long account,
                               long price, long qty, long counterOrderId, long counterAccount) {
        if (!leader) {
            return; // 只有 leader 对外发布；follower 仅推进状态
        }
        encodeEvent(eventBuf, 0, type, aux, seq, ts, orderId, account, price, qty, counterOrderId, counterAccount);
        // 撮合线程绝不自旋等待：IPC 背压时丢弃并计数，缺口由下游序号检测、由日志重放补数
        final long r = eventPublication.offer(eventView, 0, EVENT_LENGTH);
        if (r < 0) {
            droppedEvents++;
        }
        if (currentSession != null) {
            currentSession.offer(eventView, 0, EVENT_LENGTH); // 请求方确认/成交回报，失败不影响状态
        }
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {}

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        final byte[] state = engine.snapshot();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(SnapshotFraming.HEADER + SnapshotFraming.CHUNK);
        for (int off = 0; off < state.length; off += SnapshotFraming.CHUNK) {
            final int len = SnapshotFraming.encodeChunk(buf, state, off);
            long r;
            while ((r = snapshotPublication.offer(buf, 0, len)) < 0) {
                if (r == Publication.CLOSED || r == Publication.MAX_POSITION_EXCEEDED) {
                    throw new IllegalStateException("snapshot publication failed: " + r);
                }
                cluster.idleStrategy().idle(); // 快照允许阻塞，不在撮合热路径上
            }
        }
    }

    private void loadSnapshot(Image image) {
        final SnapshotFraming.Reader reader = new SnapshotFraming.Reader();
        final FragmentHandler h = (buffer, offset, length, header) -> reader.accept(buffer, offset, length);
        while (!reader.complete()) {
            if (image.poll(h, 10) == 0) {
                if (image.isClosed() || image.isEndOfStream()) {
                    break;
                }
                cluster.idleStrategy().idle();
            }
        }
        engine = MatchingEngine.fromSnapshot(reader.bytes());
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) { role = newRole; }

    @Override
    public void onTerminate(Cluster cluster) {}

    /** 测试/运维：当前状态哈希（仅在业务线程之外读取，仅用于停机后的核对）。 */
    public String stateHash() { return engine.stateHash(); }

    public long droppedEvents() { return droppedEvents; }

    public Cluster.Role role() { return role; }

    public boolean loadedFromSnapshot() { return loadedFromSnapshot; }
}
