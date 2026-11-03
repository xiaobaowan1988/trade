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
    private volatile long egressDropped;
    private volatile boolean loadedFromSnapshot;
    private volatile Cluster.Role role = Cluster.Role.FOLLOWER;

    // ---- 事件发布缓冲背压策略（“可以慢，不能丢”）----
    private long eventWaitNs = java.util.concurrent.TimeUnit.SECONDS.toNanos(3);
    private long degradedNs = java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
    private long degradedUntilNs;
    private String eventChannel = EventBridge.CHANNEL;
    private volatile long eventWaits;
    private volatile long eventWaitedNanos;
    private volatile long degradedEntries;

    /** 背压时撮合线程最多等多久（期间 cluster.idle() 保持心跳）。超时才丢弃并计数，丢弃的事件由日志补回。 */
    public ExchangeService eventWait(long waitNanos, long degradedNanos) {
        this.eventWaitNs = waitNanos;
        this.degradedNs = degradedNanos;
        return this;
    }

    /** 事件 IPC 流的 channel（测试里用来缩小缓冲）。 */
    public ExchangeService eventChannel(String channel) {
        this.eventChannel = channel;
        return this;
    }

    public String eventChannel() { return eventChannel; }

    private final jp.trade.exchange.core.EventHandler handler = this::onEngineEvent;

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        this.eventPublication = cluster.aeron().addPublication(eventChannel, EventBridge.STREAM_ID);
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
        // 先有界等待，再丢弃（设计 §0）：
        //  - ADMIN_ACTION（缓冲轮转）立即重试；
        //  - 真正背压：等下游消费，期间 cluster.idle() 保持服务心跳；
        //  - 等满 eventWaitNs 仍不行（下游挂了）：丢弃并计数，进入降级窗口，窗口内不再逐条傻等，
        //    否则下游挂掉时每条命令都会被拖住一个超时。丢弃的事件由下游发现缺口后从 Raft 日志补回。
        if (!offerEvent()) {
            droppedEvents++;
        }
        if (currentSession != null) {
            // 请求方确认/成交回报：尽力而为，背压时丢弃（不影响状态，也不阻塞撮合线程），客户端必须靠超时 + 幂等重试对账
            if (offerRetryingAdminAction(currentSession, eventView) < 0) {
                egressDropped++;
            }
        }
    }

    private boolean offerEvent() {
        long r = offerRetryingAdminAction(eventPublication, eventView);
        if (r >= 0) {
            return true;
        }
        final long now = System.nanoTime();
        if (now < degradedUntilNs || eventWaitNs <= 0) {
            return false; // 降级窗口内：不等待
        }
        final long start = now;
        eventWaits++;
        while (r < 0 && r != Publication.CLOSED && r != Publication.MAX_POSITION_EXCEEDED
                && System.nanoTime() - start < eventWaitNs) {
            cluster.idleStrategy().idle(); // 服务专用的 idle：持续检查时钟/终止并维持心跳
            r = offerRetryingAdminAction(eventPublication, eventView);
        }
        eventWaitedNanos += System.nanoTime() - start;
        if (r >= 0) {
            return true;
        }
        degradedUntilNs = System.nanoTime() + degradedNs;
        degradedEntries++;
        return false;
    }

    interface Offerer {
        long offer(org.agrona.DirectBuffer buffer, int offset, int length);
    }

    private static long offerRetryingAdminAction(Publication pub, DirectBuffer buf) {
        return offerRetryingAdminAction(pub::offer, buf);
    }

    private static long offerRetryingAdminAction(ClientSession session, DirectBuffer buf) {
        return offerRetryingAdminAction(session::offer, buf);
    }

    /** ADMIN_ACTION（term 轮转）立即重试最多 3 次；BACK_PRESSURED 等其他负值不重试，直接返回。 */
    static long offerRetryingAdminAction(Offerer o, DirectBuffer buf) {
        long r = o.offer(buf, 0, EVENT_LENGTH);
        for (int i = 0; i < 3 && r == Publication.ADMIN_ACTION; i++) {
            r = o.offer(buf, 0, EVENT_LENGTH);
        }
        return r;
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

    /** 撮合线程因事件发布背压而进入等待的次数 / 累计等待纳秒 / 因等待超时进入降级窗口的次数。 */
    public long eventWaits() { return eventWaits; }
    public long eventWaitedNanos() { return eventWaitedNanos; }
    public long degradedEntries() { return degradedEntries; }

    /** 因 egress 背压而没能送达请求方的确认/回报条数。 */
    public long egressDropped() { return egressDropped; }

    public Cluster.Role role() { return role; }

    public boolean loadedFromSnapshot() { return loadedFromSnapshot; }
}
