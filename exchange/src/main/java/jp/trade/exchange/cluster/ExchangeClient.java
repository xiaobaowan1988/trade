package jp.trade.exchange.cluster;

import io.aeron.CommonContext;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.samples.cluster.ClusterConfig;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;

import static jp.trade.exchange.core.Protocol.*;

/** 简单同步客户端：提交命令，并收集集群通过 egress 回写的确认/成交回报。 */
public final class ExchangeClient implements AutoCloseable, EgressListener {
    private final AeronCluster cluster;
    private final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(COMMAND_LENGTH);
    private final List<byte[]> responses = new ArrayList<>();
    private final SleepingIdleStrategy idle = new SleepingIdleStrategy(1_000_000);

    private final MediaDriver ownedDriver;

    /** 连接单成员集群，复用该节点的 MediaDriver。 */
    public ExchangeClient(String aeronDir) {
        this(aeronDir, 1, null, DEFAULT_CONNECT_TIMEOUT_MS);
    }

    /**
     * 连接超时必须大于驱动的发布连接超时（节点默认 60 秒）：首次连接没成功的发布端要等满那么久才会重试，
     * 客户端若只等 5 秒就会在重试发生之前放弃。需要“连不上就快速失败”的场景（例如测试）可以传一个较短的值。
     */
    public static final long DEFAULT_CONNECT_TIMEOUT_MS = 90_000;

    public ExchangeClient(String aeronDir, long connectTimeoutMs) {
        this(aeronDir, 1, null, connectTimeoutMs);
    }

    /** 连接 memberCount 成员集群，客户端自带独立 MediaDriver，不依赖任何一个集群节点存活。 */
    public static ExchangeClient connect(int memberCount) {
        final MediaDriver d = MediaDriver.launch(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true)
                .aeronDirectoryName(CommonContext.getAeronDirectoryName() + "-client-" + System.nanoTime()));
        return new ExchangeClient(d.aeronDirectoryName(), memberCount, d, DEFAULT_CONNECT_TIMEOUT_MS);
    }

    private ExchangeClient(String aeronDir, int memberCount, MediaDriver ownedDriver, long connectTimeoutMs) {
        this.ownedDriver = ownedDriver;
        this.cluster = connectWithRetry(aeronDir, memberCount, connectTimeoutMs);
    }

    /**
     * 连接失败就换一个全新的发布端重试，直到总期限。不依赖驱动里“发布连接超时”去触发内部重试：
     * 节点刚启动、或刚经历 leader 切换时，第一次尝试失败很常见，而节点默认把发布连接超时放宽到了 60 秒。
     * 每次尝试只等一小段时间（默认 5 秒；总期限比它短时用总期限）。
     */
    private AeronCluster connectWithRetry(String aeronDir, int memberCount, long totalMs) {
        final long deadline = System.currentTimeMillis() + totalMs;
        final long attemptMs = Math.min(5_000, totalMs);
        RuntimeException last = null;
        do {
            try {
                return AeronCluster.connect(new AeronCluster.Context()
                        .aeronDirectoryName(aeronDir)
                        .messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(attemptMs))
                        .egressListener(this)
                        .ingressChannel("aeron:udp?term-length=64k")
                        .ingressEndpoints(ClusterConfig.ingressEndpoints(
                                ExchangeNode.hostnames(memberCount), ExchangeNode.PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                        .egressChannel("aeron:udp?endpoint=localhost:0"));
            } catch (io.aeron.exceptions.TimeoutException e) {
                last = e;
            }
        } while (System.currentTimeMillis() < deadline);
        throw last;
    }

    @Override
    public void onMessage(long clusterSessionId, long timestamp, DirectBuffer buffer, int offset, int length, Header header) {
        final byte[] b = new byte[length];
        buffer.getBytes(offset, b);
        responses.add(b);
    }

    private void send(int type, int aux, long account, long f1, long f2, long f3) {
        final int len = encodeCommand(buf, 0, type, aux, account, f1, f2, f3);
        long r;
        final long deadline = System.currentTimeMillis() + 30_000;
        while ((r = cluster.offer(buf, 0, len)) < 0) {
            // NOT_CONNECTED/ADMIN_ACTION 等在 leader 切换期间是暂时的：继续 pollEgress 以接收新 leader 通知后重试
            if (r == io.aeron.Publication.CLOSED || r == io.aeron.Publication.MAX_POSITION_EXCEEDED || System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("offer failed: " + r);
            }
            cluster.pollEgress();
            idle.idle();
        }
    }

    /** 仅测试：发送任意字节（例如构造恶意消息）。 */
    public void sendRawForTest(byte[] payload) {
        final ExpandableArrayBuffer b = new ExpandableArrayBuffer(payload.length);
        b.putBytes(0, payload);
        long r;
        while ((r = cluster.offer(b, 0, payload.length)) < 0) {
            if (r == io.aeron.Publication.CLOSED) {
                throw new IllegalStateException("closed");
            }
            cluster.pollEgress();
        }
    }

    /** 入金（自动生成随机 depositId；需要超时重试同一笔入金时用带 depositId 的重载）。 */
    public void deposit(long account, int asset, long amount) {
        deposit(account, asset, amount, java.util.concurrent.ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE));
    }

    /** 入金是幂等的：同一 (account, depositId) 只会入账一次，重试返回 REJECTED(DUPLICATE_DEPOSIT)。 */
    public void deposit(long account, int asset, long amount, long depositId) {
        send(CMD_DEPOSIT, asset, account, amount, depositId, 0);
    }

    public void place(long account, int side, long clientOrderId, long price, long qty) {
        send(CMD_PLACE, side, account, clientOrderId, price, qty);
    }

    public void cancel(long account, long orderId) { send(CMD_CANCEL, 0, account, orderId, 0, 0); }

    /** 等待至少 n 条回报（超时抛异常），返回并清空已收到的回报。 */
    public List<byte[]> await(int n, long timeoutMs) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (responses.size() < n) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("timeout, got " + responses.size() + " of " + n);
            }
            if (cluster.pollEgress() == 0) {
                idle.idle();
            }
        }
        final List<byte[]> out = new ArrayList<>(responses);
        responses.clear();
        return out;
    }

    @Override
    public void close() {
        cluster.close();
        if (ownedDriver != null) {
            ownedDriver.close();
        }
    }
}
