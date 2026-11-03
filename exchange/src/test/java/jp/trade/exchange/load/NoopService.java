package jp.trade.exchange.load;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.HashMap;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.AgentTerminationException;

/**
 * 空服务：不撮合、不产生事件，只做两件事——
 *  1. 证明定序的正确性：每个发送方的序号必须严格连续（不丢、不重、不乱序），违反即 fail-stop；
 *  2. 对“全局顺序”维护链式哈希，副本之间、重放之后必须得到同一个值。
 * 这样测出来的吞吐只属于 Aeron Cluster 的定序（接入 → Raft 日志 → 多数派确认 → 交付给服务）。
 *
 * 消息（48 字节，与真实命令等长）：int magic | int pad | long gatewayId | long seq | 24 字节填充
 */
final class NoopService implements ClusteredService {
    static final int MAGIC = 0x4E4F4F50; // "NOOP"
    static final int MSG_LENGTH = 48;
    static final int ACK_LENGTH = 16;

    private Cluster cluster;
    private final Map<Long, Long> lastSeq = new HashMap<>();
    private final ExpandableArrayBuffer ack = new ExpandableArrayBuffer(ACK_LENGTH);
    private final int ackEvery;

    volatile long count;
    volatile long chain;
    volatile long violations;
    volatile String firstViolation;
    volatile boolean halted;
    volatile Cluster.Role role = Cluster.Role.FOLLOWER;
    volatile boolean loadedFromSnapshot;

    NoopService(int ackEvery) {
        this.ackEvery = ackEvery;
    }

    private static long mix(long h, long x) {
        h ^= x;
        h *= 0x9E3779B97F4A7C15L;
        h ^= h >>> 32;
        return h;
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        if (snapshotImage != null) {
            final long[] st = new long[4];
            final boolean[] got = {false};
            final FragmentHandler h = (b, o, l, hd) -> {
                st[0] = b.getLong(o);
                st[1] = b.getLong(o + 8);
                got[0] = true;
            };
            while (!got[0]) {
                if (snapshotImage.poll(h, 1) == 0) {
                    cluster.idleStrategy().idle();
                }
            }
            count = st[0];
            chain = st[1];
            loadedFromSnapshot = true;
        }
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {}

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {}

    @Override
    public void onSessionMessage(ClientSession session, long timestamp, DirectBuffer buf, int offset, int length, Header header) {
        if (session == null) {
            return;
        }
        if (length != MSG_LENGTH || buf.getInt(offset) != MAGIC) {
            fail("malformed message length=" + length);
            return;
        }
        final long gateway = buf.getLong(offset + 8);
        final long seq = buf.getLong(offset + 16);
        final long expected = lastSeq.getOrDefault(gateway, 0L) + 1;
        if (seq != expected) {
            // 丢失(seq>expected)、重复或乱序(seq<expected)：定序出错，必须停下，绝不继续
            fail("gateway " + gateway + " expected seq " + expected + " but got " + seq);
            return;
        }
        lastSeq.put(gateway, seq);
        chain = mix(chain, (gateway << 40) ^ seq);
        count++;
        if (ackEvery > 0 && seq % ackEvery == 0 && cluster.role() == Cluster.Role.LEADER) {
            ack.putLong(0, gateway);
            ack.putLong(8, seq);
            long r = session.offer(ack, 0, ACK_LENGTH);
            for (int i = 0; i < 3 && r == Publication.ADMIN_ACTION; i++) {
                r = session.offer(ack, 0, ACK_LENGTH);
            }
        }
    }

    private void fail(String what) {
        violations++;
        firstViolation = what;
        halted = true;
        throw new AgentTerminationException("sequencing violation: " + what);
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {}

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        final ExpandableArrayBuffer b = new ExpandableArrayBuffer(16);
        b.putLong(0, count);
        b.putLong(8, chain);
        while (snapshotPublication.offer(b, 0, 16) < 0) {
            cluster.idleStrategy().idle();
        }
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) { role = newRole; }

    @Override
    public void onTerminate(Cluster cluster) {}
}
