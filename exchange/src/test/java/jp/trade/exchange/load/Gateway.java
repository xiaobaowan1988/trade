package jp.trade.exchange.load;

import static jp.trade.exchange.core.Protocol.*;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.ClusterConfig;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import jp.trade.exchange.cluster.ExchangeNode;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * 模拟网关：一个线程、一个集群会话，复用承载一批用户账户的请求（设计 §1：用户不直接连集群）。
 * 非阻塞发送 + 在途窗口；用 egress 回报里的 clientOrderId 关联延迟。
 */
final class Gateway implements Runnable, EgressListener {
    final int id;
    final int[] accounts;
    private final AeronCluster cluster;
    private final Random rnd;
    private final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(COMMAND_LENGTH);

    // 阶段控制（由 LoadRunner 在运行期间切换）
    volatile long targetPlaces;        // 本阶段要发的下单数
    volatile double ratePerSecond;     // 0 = 不限速（仅受窗口约束）
    volatile int window;
    volatile boolean stop;
    private volatile int phase;        // 递增即开始新阶段
    private volatile boolean phaseDone = true;

    // 统计（仅本线程写；阶段结束后由主线程读）
    long[] latencies = new long[0];
    int latCount;
    volatile long placesSent, accepted, rejectedPlaces, deposited, lostPlaces;
    long lateResponses;
    private volatile long lastEgressNs = System.nanoTime();
    long cancelsSent, cancelled, cancelRejected, trades;
    long backPressure, notConnected, adminAction, otherOfferFail;
    long rejectsByReason[] = new long[9];

    // ---- 重试与客户端视角（用于验证“重试取回原结果、不重复生效”）----
    volatile double retryProbability;          // 每笔下单被确认后，再用同一个 clientOrderId 重发一次的概率（模拟“回报丢了，超时重试”）
    volatile double depositRetryProbability;
    private final Map<Long, long[]> retryParams = new HashMap<>();          // key → {side, price, qty}，等回报到达后重发
    private final java.util.ArrayDeque<long[]> retryQueue = new java.util.ArrayDeque<>(); // {account, cid, side, price, qty}
    /** 客户端观察到的每个 (账户, clientOrderId) 的结果：>0 订单号，<0 拒绝原因的负数。 */
    final Map<Long, Long> observed = new HashMap<>();
    private final Map<Long, Long> depositAmount = new HashMap<>();          // (账户<<3|资产) → 金额
    long retriesSent, dupChecked, dupMismatch, dupLearned, depositDupChecked, depositDupMismatch;
    private final Map<Long, Long> sentAt = new HashMap<>();             // key=account<<32|cid → 预定发送时间
    private final Map<Integer, ArrayDeque<Long>> resting = new HashMap<>();
    private final Map<Integer, Integer> nextCid = new HashMap<>();

    Gateway(int id, int[] accounts, String aeronDir, int memberCount, long seed) {
        this(id, accounts, aeronDir, null, memberCount, seed);
    }

    /** 共享同一个 Aeron 客户端（几百个会话时避免每个会话各起一套客户端线程）。 */
    Gateway(int id, int[] accounts, io.aeron.Aeron shared, int memberCount, long seed) {
        this(id, accounts, null, shared, memberCount, seed);
    }

    private Gateway(int id, int[] accounts, String aeronDir, io.aeron.Aeron shared, int memberCount, long seed) {
        this.id = id;
        this.accounts = accounts;
        this.rnd = new Random(seed);
        final AeronCluster.Context ctx = new AeronCluster.Context()
                .egressListener(this)
                .ingressChannel("aeron:udp?term-length=" + System.getProperty("ingressTerm", "1m"))
                .ingressEndpoints(ClusterConfig.ingressEndpoints(
                        ExchangeNode.hostnames(memberCount), ExchangeNode.PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                .egressChannel("aeron:udp?endpoint=localhost:0|term-length=" + System.getProperty("egressTerm", "1m"))
                .messageTimeoutNs(java.util.concurrent.TimeUnit.SECONDS.toNanos(10)); // 每次尝试 10 秒；失败则换新的发布端重试，见 connectWithRetry
        if (shared != null) {
            ctx.aeron(shared).ownsAeronClient(false);
        } else {
            ctx.aeronDirectoryName(aeronDir);
        }
        this.cluster = connectWithRetry(ctx, aeronDir, shared, memberCount);
    }

    /** 连接失败就换新的发布端重试（总期限 connectTimeoutSec）。几百个会话同时建立时，个别连接失败很常见。 */
    private AeronCluster connectWithRetry(AeronCluster.Context first, String aeronDir, io.aeron.Aeron shared, int memberCount) {
        final long deadline = System.currentTimeMillis() + Long.getLong("connectTimeoutSec", 120) * 1000;
        RuntimeException last = null;
        AeronCluster.Context ctx = first;
        while (true) {
            try {
                return AeronCluster.connect(ctx);
            } catch (io.aeron.exceptions.TimeoutException e) {
                last = e;
                if (System.currentTimeMillis() > deadline) {
                    throw last;
                }
                ctx = new AeronCluster.Context()
                        .egressListener(this)
                        .ingressChannel("aeron:udp?term-length=" + System.getProperty("ingressTerm", "1m"))
                        .ingressEndpoints(ClusterConfig.ingressEndpoints(
                                ExchangeNode.hostnames(memberCount), ExchangeNode.PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                        .egressChannel("aeron:udp?endpoint=localhost:0|term-length=" + System.getProperty("egressTerm", "1m"))
                        .messageTimeoutNs(java.util.concurrent.TimeUnit.SECONDS.toNanos(10));
                if (shared != null) {
                    ctx.aeron(shared).ownsAeronClient(false);
                } else {
                    ctx.aeronDirectoryName(aeronDir);
                }
            }
        }
    }

    // ---------- egress ----------

    @Override
    public void onMessage(long sessionId, long timestamp, DirectBuffer b, int offset, int length, Header header) {
        if (length != EVENT_LENGTH) {
            return;
        }
        lastEgressNs = System.nanoTime();
        final int type = eventType(b, offset);
        final long account = eventAccount(b, offset);
        switch (type) {
            case EVT_DEPOSITED -> deposited++;
            case EVT_ACCEPTED -> {
                accepted++;
                final long cid = eventCounterOrderId(b, offset);
                final long pk = (account << 32) | cid;
                observed.putIfAbsent(pk, eventOrderId(b, offset));
                final long[] rp = retryParams.remove(pk);
                if (rp != null) {
                    retryQueue.add(new long[]{account, cid, rp[0], rp[1], rp[2]});
                }
                if (!record(account, cid)) {
                    late();
                }
                resting.computeIfAbsent((int) account, k -> new ArrayDeque<>()).addLast(eventOrderId(b, offset));
                final ArrayDeque<Long> q = resting.get((int) account);
                if (q.size() > 16) {
                    q.pollFirst();
                }
            }
            case EVT_REJECTED -> {
                final long reason = eventQty(b, offset);
                rejectsByReason[(int) Math.min(reason, 8)]++;
                if (reason == REJECT_DUPLICATE_DEPOSIT) {
                    // 入金重试：必须返回原金额，且不再入账
                    depositDupChecked++;
                    final Long expected = depositAmount.get((account << 3) | eventCounterOrderId(b, offset));
                    if (expected == null || expected != eventPrice(b, offset)) {
                        depositDupMismatch++;
                    }
                } else if (reason == REJECT_UNKNOWN_ORDER || reason == REJECT_DUPLICATE_CANCEL) {
                    cancelRejected++;
                } else {
                    rejectedPlaces++;
                    final long cid = eventCounterOrderId(b, offset);
                    final long pk = (account << 32) | cid;
                    if (reason == REJECT_DUPLICATE_CLIENT_ORDER) {
                        // 重试得到的“原结果”：原订单号（或原拒绝原因）必须与我们之前看到的一致
                        dupChecked++;
                        final long claimed = eventOrderId(b, offset) > 0 ? eventOrderId(b, offset) : -eventPrice(b, offset);
                        final Long seen = observed.get(pk);
                        if (seen == null) {
                            observed.put(pk, claimed); // 原回报丢了，靠重试取回
                            dupLearned++;
                        } else if (seen != claimed) {
                            dupMismatch++;
                        }
                    } else {
                        observed.putIfAbsent(pk, -reason);
                    }
                    if (!record(account, cid)) {
                        late();
                    }
                }
            }
            case EVT_CANCELLED -> cancelled++;
            case EVT_TRADE -> trades++;
            default -> { }
        }
    }

    private boolean record(long account, long cid) {
        final Long t0 = sentAt.remove((account << 32) | cid);
        if (t0 == null) {
            return false;
        }
        {
            if (latCount == latencies.length) {
                latencies = java.util.Arrays.copyOf(latencies, Math.max(1024, latencies.length * 2));
            }
            latencies[latCount++] = System.nanoTime() - t0;
        }
        return true;
    }

    /** 已被判定为丢失的请求，回报晚到了：冲正。 */
    private void late() {
        lateResponses++;
        lostPlaces--;
    }

    // ---------- 控制 ----------

    void startPhase(long places, double rate, int window) {
        this.targetPlaces = places;
        this.ratePerSecond = rate;
        this.window = window;
        this.phaseDone = false;
        this.phase++;
    }

    boolean phaseDone() { return phaseDone; }

    boolean clusterClosed() { return cluster.isClosed(); }

    /** 建立大量会话期间，对先建好的会话保活（否则它们在工作线程启动前就超时了）。只能在工作线程启动前由单个线程调用。 */
    void keepAliveBeforeStart() {
        cluster.sendKeepAlive();
    }

    /** 清零统计，阶段之间调用（此时线程处于空闲）。 */
    void resetStats() {
        latCount = 0;
        placesSent = accepted = rejectedPlaces = lostPlaces = 0;
        lateResponses = 0;
        cancelsSent = cancelled = cancelRejected = trades = 0;
        backPressure = notConnected = adminAction = otherOfferFail = 0;
        java.util.Arrays.fill(rejectsByReason, 0);
    }

    private final ArrayDeque<long[]> depositQueue = new ArrayDeque<>();
    volatile long depositsSent;

    /** 排队开户入金（在 run() 线程里按窗口发出，边发边消费 egress）。 */
    void queueDeposits(long quote, long base) {
        for (int a : accounts) {
            depositQueue.add(new long[]{a, ASSET_QUOTE, quote});
            depositQueue.add(new long[]{a, ASSET_BASE, base});
        }
    }

    volatile long depositRetriesSent;

    private void pumpDeposits() {
        // 重试的入金不会产生 DEPOSITED 确认，所以窗口要扣掉它们
        for (int i = 0; i < 64 && !depositQueue.isEmpty() && depositsSent - depositRetriesSent - deposited < 500; i++) {
            final long[] d = depositQueue.peekFirst();
            final boolean isRetry = d.length == 4;
            final int len = encodeCommand(buf, 0, CMD_DEPOSIT, (int) d[1], d[0], d[2], d[1], 0); // depositId=资产号，每账户每资产一笔
            if (cluster.offer(buf, 0, len) < 0) {
                backPressure++;
                return;
            }
            depositQueue.pollFirst();
            depositsSent++;
            if (isRetry) {
                depositRetriesSent++;
            } else {
                depositAmount.put((d[0] << 3) | d[1], d[2]);
                if (rnd.nextDouble() < depositRetryProbability) {
                    depositQueue.addFirst(new long[]{d[0], d[1], d[2], 1}); // 同一个 depositId 再发一次（同一会话保证它在原件之后被处理）
                }
            }
        }
    }

    // 运行状态（由 step() 维护，使多个网关可以复用同一个线程）
    private long lastKeepAlive = System.nanoTime();
    private int seenPhase;
    private long sentThisPhase;
    private long nextSlot;
    private long phaseStart;
    volatile boolean sessionClosedSeen;

    @Override
    public void run() {
        final IdleStrategy idle = new BackoffIdleStrategy(100, 10, 1_000, 200_000);
        while (!stop) {
            idle.idle(step());
        }
        closeCluster();
    }

    void closeCluster() {
        try {
            cluster.close();
        } catch (RuntimeException ignored) {
        }
    }

    /** 一轮：消费 egress、发开户入金、按窗口发命令、保活。返回做了多少工作（用于空闲退避）。 */
    int step() {
        {
            if (cluster.isClosed()) {
                sessionClosedSeen = true;
                return 0;
            }
            if (phase != seenPhase) {
                seenPhase = phase;
                sentThisPhase = 0;
                phaseStart = System.nanoTime();
                nextSlot = phaseStart;
            }
            int work = cluster.pollEgress();
            if (!depositQueue.isEmpty()) {
                pumpDeposits();
                work++;
            }

            if (!phaseDone) {
                final long now = System.nanoTime();
                final double rate = ratePerSecond;
                final long intervalNs = rate > 0 ? (long) (1e9 / rate) : 0;
                // 一次最多连发一批，避免饿死 egress 轮询
                for (int burst = 0; burst < 64 && sentThisPhase < targetPlaces; burst++) {
                    final long inFlight = placesSent - accepted - rejectedPlaces - lostPlaces;
                    if (inFlight >= window) {
                        break;
                    }
                    final long intended;
                    if (intervalNs > 0) {
                        if (System.nanoTime() < nextSlot) {
                            break;
                        }
                        intended = nextSlot;
                    } else {
                        intended = System.nanoTime();
                    }
                    if (!sendOne(intended)) {
                        break; // 背压：先去 poll egress
                    }
                    sentThisPhase++;
                    if (intervalNs > 0) {
                        nextSlot += intervalNs;
                    }
                    work++;
                }
                if (sentThisPhase >= targetPlaces && placesSent - accepted - rejectedPlaces - lostPlaces <= 0) {
                    phaseDone = true; // 全部下单都已得到回报
                }
            }
            // 3 秒没有任何 egress 回报而仍有在途请求：判定为回报丢失（服务端 egress 背压时会丢弃）
            final long outstanding = placesSent - accepted - rejectedPlaces - lostPlaces;
            if (outstanding > 0 && System.nanoTime() - lastEgressNs > 3_000_000_000L) {
                lostPlaces += outstanding;
                sentAt.clear();
                lastEgressNs = System.nanoTime();
            }
            if (System.nanoTime() - lastKeepAlive > 1_000_000_000L) {
                cluster.sendKeepAlive();
                lastKeepAlive = System.nanoTime();
            }
            return work;
        }
    }

    private boolean sendOne(long intendedNs) {
        if (!retryQueue.isEmpty()) {
            final long[] r = retryQueue.peekFirst();
            final int len = encodeCommand(buf, 0, CMD_PLACE, (int) r[2], r[0], r[1], r[3], r[4]);
            final long res = cluster.offer(buf, 0, len);
            if (res < 0) {
                backPressure++;
                return false;
            }
            retryQueue.pollFirst();
            placesSent++;
            retriesSent++;
            sentAt.put((r[0] << 32) | r[1], intendedNs);
            return true;
        }
        final int account = accounts[rnd.nextInt(accounts.length)];
        final boolean cancel = rnd.nextInt(10) == 0 && resting.containsKey(account) && !resting.get(account).isEmpty();
        final long result;
        if (cancel) {
            final long orderId = resting.get(account).pollFirst();
            final int len = encodeCommand(buf, 0, CMD_CANCEL, 0, account, orderId, 0, 0);
            result = cluster.offer(buf, 0, len);
            if (result >= 0) {
                cancelsSent++;
            } else {
                resting.get(account).addFirst(orderId);
            }
        } else {
            final int cid = nextCid.merge(account, 1, Integer::sum);
            final int side = rnd.nextBoolean() ? SIDE_BUY : SIDE_SELL;
            final long price = 100_000 + rnd.nextInt(101) - 50;
            final long qty = 1 + rnd.nextInt(10);
            final int len = encodeCommand(buf, 0, CMD_PLACE, side, account, cid, price, qty);
            result = cluster.offer(buf, 0, len);
            if (result >= 0) {
                placesSent++;
                sentAt.put(((long) account << 32) | cid, intendedNs);
                if (rnd.nextDouble() < retryProbability) {
                    retryParams.put(((long) account << 32) | cid, new long[]{side, price, qty});
                }
            } else {
                nextCid.merge(account, -1, Integer::sum); // 没发出去，cid 回退
            }
        }
        if (result < 0) {
            if (result == Publication.BACK_PRESSURED) {
                backPressure++;
            } else if (result == Publication.NOT_CONNECTED) {
                notConnected++;
            } else if (result == Publication.ADMIN_ACTION) {
                adminAction++;
            } else {
                otherOfferFail++;
            }
            return false;
        }
        return true;
    }
}
