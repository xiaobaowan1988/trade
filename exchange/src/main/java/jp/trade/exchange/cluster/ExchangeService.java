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
import org.agrona.concurrent.AgentTerminationException;
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
    // ---- 链式哈希检查点（设计 §0 第 6 点）----
    private static final int CHECK_MAGIC = CHECKPOINT_MAGIC;
    private static final int CHECK_LENGTH = CHECKPOINT_LENGTH;
    private final java.util.TreeMap<Long, Long> pendingChecks = new java.util.TreeMap<>();
    private final long[] checkpointOut = new long[2];
    private final ExpandableArrayBuffer checkBuf = new ExpandableArrayBuffer(CHECK_LENGTH);
    private volatile long checkpointsSent;
    private volatile long checkpointsFailed;
    private long checkpointWaitNs = java.util.concurrent.TimeUnit.SECONDS.toNanos(1);
    private volatile long checkpointsVerified;
    private volatile long checkpointsUnverifiable;
    /** 仅测试：按日志实际应用的顺序拿到每条命令（用来喂独立的参考实现做差分验证）。 */
    public interface CommandTap {
        void onCommand(long timestamp, byte[] command);
    }

    private volatile CommandTap commandTap;
    private volatile boolean halted;
    private volatile Throwable fatalCause;
    private java.util.function.Consumer<Throwable> fatalHandler = ExchangeService::haltJvm;
    /** 测试用故障注入：处理第 N 条命令时抛异常（0=关闭）。 */
    private volatile long injectFailureAtCommand;
    private volatile long commandCount;
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
            try {
                loadSnapshot(snapshotImage);
                engine.verifyInvariants(); // 加载出来的状态也要自洽，否则宁可起不来
            } catch (Throwable t) {
                fatal("cannot load a trustworthy snapshot", t);
            }
            loadedFromSnapshot = true;
        }
    }

    private volatile long sessionsOpened;
    private volatile long sessionsClosed;
    private final java.util.concurrent.ConcurrentHashMap<String, Long> closeReasons = new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        sessionsOpened++;
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        sessionsClosed++;
        closeReasons.merge(String.valueOf(closeReason), 1L, Long::sum);
    }

    public long sessionsOpened() { return sessionsOpened; }
    public long sessionsClosed() { return sessionsClosed; }

    /** 会话被关闭的原因及次数（CLIENT_ACTION、TIMEOUT、SERVICE_ACTION 等）。 */
    public java.util.Map<String, Long> closeReasons() { return java.util.Map.copyOf(closeReasons); }

    @Override
    public void onSessionMessage(ClientSession session, long timestamp, DirectBuffer buffer, int offset, int length, Header header) {
        if (halted) {
            throw new AgentTerminationException("service is halted (fail-stop)");
        }
        try {
            if (session == null && length == CHECK_LENGTH && buffer.getInt(offset) == CHECK_MAGIC) {
                // 服务自己发的消息到达时 session 为 null；客户端会话永远非 null，所以客户端伪造不了检查点
                // 日志里的检查点消息：用本副本自己算出的值核对
                onCheckpointMessage(session, buffer, offset);
                return;
            }
            leader = cluster.role() == Cluster.Role.LEADER;
            currentSession = leader ? session : null;
            if (++commandCount == injectFailureAtCommand) {
                throw new IllegalStateException("injected failure at command " + commandCount);
            }
            final CommandTap tap = commandTap;
            if (tap != null) {
                final byte[] copy = new byte[length];
                buffer.getBytes(offset, copy);
                tap.onCommand(timestamp, copy);
            }
            engine.onCommand(buffer, offset, length, timestamp, handler);
            if (engine.takeCheckpoint(checkpointOut)) {
                sendCheckpoint(checkpointOut[0], checkpointOut[1]);
            }
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Throwable t) { // 包括 Error（OOM 等）。绝不“记录一下继续”，见设计 §0 第 3 点
            fatal("state machine failed while applying command #" + commandCount, t);
        } finally {
            currentSession = null;
        }
    }

    private void sendCheckpoint(long seq, long chain) {
        pendingChecks.put(seq, chain);
        while (pendingChecks.size() > 10_000) {
            pendingChecks.pollFirstEntry(); // 有界：发出去却一直没收到的检查点不会让内存无限增长
        }
        checkBuf.putInt(0, CHECK_MAGIC);
        checkBuf.putLong(4, seq);
        checkBuf.putLong(12, chain);
        // 所有副本在同一个逻辑位置做同一个调用；Aeron 保证日志里只追加一份（以 leader 的为准）。
        // 满载时 offer 可能被背压拒绝：leader 上有界等待重试（可以慢），否则这个检查点会静默缺失、悄悄削弱检测覆盖。
        // 只有 leader 的结果有意义，follower 不等待。
        final long maxWait = cluster.role() == Cluster.Role.LEADER ? checkpointWaitNs : 0;
        final long r = offerBounded(cluster::offer, checkBuf, CHECK_LENGTH, maxWait, () -> cluster.idleStrategy().idle());
        if (r >= 0) {
            checkpointsSent++;
        } else if (cluster.role() == Cluster.Role.LEADER) {
            checkpointsFailed++;
        }
    }

    /**
     * offer 并在失败时有界等待重试：ADMIN_ACTION 立即重试；BACK_PRESSURED/NOT_CONNECTED 等待到 maxWaitNs；
     * CLOSED / MAX_POSITION_EXCEEDED 立即放弃（重试没有意义）。
     */
    static long offerBounded(Offerer o, DirectBuffer buf, int length, long maxWaitNs, Runnable idle) {
        long r = o.offer(buf, 0, length);
        final long deadline = System.nanoTime() + maxWaitNs;
        while (r < 0 && r != Publication.CLOSED && r != Publication.MAX_POSITION_EXCEEDED && System.nanoTime() < deadline) {
            if (r != Publication.ADMIN_ACTION) {
                idle.run();
            }
            r = o.offer(buf, 0, length);
        }
        return r;
    }

    private void onCheckpointMessage(ClientSession session, DirectBuffer buffer, int offset) {
        final long seq = buffer.getLong(offset + 4);
        final long theirs = buffer.getLong(offset + 12);
        final Long mine = pendingChecks.remove(seq);
        if (mine == null) {
            checkpointsUnverifiable++; // 例如从快照恢复后收到快照之前发出的检查点：无法核对，不算通过
            return;
        }
        if (mine != theirs) {
            throw new IllegalStateException("checkpoint mismatch at event seq " + seq + ": this replica computed " + mine
                    + " but the log recorded " + theirs + " (replica divergence, log corruption, or changed semantics)");
        }
        checkpointsVerified++;
    }

    public long checkpointsSent() { return checkpointsSent; }

    /** leader 上因 offer 一直被拒而未能写入日志的检查点数（应为 0，否则该检查点缺失、检测覆盖被削弱）。 */
    public long checkpointsFailed() { return checkpointsFailed; }
    public long checkpointsVerified() { return checkpointsVerified; }
    public long checkpointsUnverifiable() { return checkpointsUnverifiable; }

    /** 仅测试：让引擎的语义与旧日志不一致。 */
    public ExchangeService skewForTest(long orderIdDelta) {
        engine.skewNextOrderIdForTest(orderIdDelta);
        return this;
    }

    /**
     * fail-stop：状态机里出现任何无法证明正确的情况都停下。AgentRunner 对普通异常只会记录并继续下一轮，
     * 而该命令的日志位置已经前移，状态可能只改了一半，所有副本还会确定性地一起错。
     * 停机不丢数据：日志里的命令还在，修复后重放即可。
     */
    private void fatal(String what, Throwable cause) {
        halted = true;
        fatalCause = cause;
        try {
            fatalHandler.accept(new IllegalStateException(what, cause));
        } catch (Throwable ignored) {
            // 处理器本身失败也必须继续停机
        }
        throw new AgentTerminationException("fail-stop: " + what);
    }

    private static void haltJvm(Throwable t) {
        System.err.println("FATAL: exchange state machine fail-stop; halting JVM so another member can take over");
        t.printStackTrace();
        System.err.flush();
        Runtime.getRuntime().halt(70);
    }

    /** 自定义致命错误处理（默认：打印并 halt JVM）。测试里用来记录而不杀掉测试进程。 */
    public ExchangeService fatalHandler(java.util.function.Consumer<Throwable> handler) {
        this.fatalHandler = handler;
        return this;
    }

    public boolean halted() { return halted; }

    /** 已应用的命令数（含被拒绝的）。 */
    public long commandsApplied() { return commandCount; }

    /** 仅测试：设置命令记录接口。 */
    public ExchangeService commandTap(CommandTap tap) {
        this.commandTap = tap;
        return this;
    }

    public Throwable fatalCause() { return fatalCause; }

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
        final byte[] state;
        try {
            engine.verifyInvariants(); // 不对违反不变量的状态做快照：一份错误的快照会被所有后续恢复继承
            state = engine.snapshot();
        } catch (Throwable t) {
            fatal("state failed pre-snapshot verification", t);
            return;
        }
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

    /** 仅测试：让第 N 条命令抛异常。 */
    public ExchangeService injectFailureAtCommand(long n) {
        this.injectFailureAtCommand = n;
        return this;
    }
}
