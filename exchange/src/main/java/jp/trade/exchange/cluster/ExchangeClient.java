package jp.trade.exchange.cluster;

import io.aeron.CommonContext;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.samples.cluster.ClusterConfig;
import java.util.ArrayList;
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
        this(aeronDir, 1, null);
    }

    /** 连接 memberCount 成员集群，客户端自带独立 MediaDriver，不依赖任何一个集群节点存活。 */
    public static ExchangeClient connect(int memberCount) {
        final MediaDriver d = MediaDriver.launch(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true)
                .aeronDirectoryName(CommonContext.getAeronDirectoryName() + "-client-" + System.nanoTime()));
        return new ExchangeClient(d.aeronDirectoryName(), memberCount, d);
    }

    private ExchangeClient(String aeronDir, int memberCount, MediaDriver ownedDriver) {
        this.ownedDriver = ownedDriver;
        this.cluster = AeronCluster.connect(new AeronCluster.Context()
                .aeronDirectoryName(aeronDir)
                .egressListener(this)
                .ingressChannel("aeron:udp?term-length=64k")
                .ingressEndpoints(ClusterConfig.ingressEndpoints(
                        ExchangeNode.hostnames(memberCount), ExchangeNode.PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                .egressChannel("aeron:udp?endpoint=localhost:0"));
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
            if (r == io.aeron.Publication.MAX_POSITION_EXCEEDED || System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("offer failed: " + r);
            }
            cluster.pollEgress();
            idle.idle();
        }
    }

    public void deposit(long account, int asset, long amount) { send(CMD_DEPOSIT, asset, account, amount, 0, 0); }

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
