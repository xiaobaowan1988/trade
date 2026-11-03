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
    long rejectsByReason[] = new long[8];

    private final Map<Long, Long> sentAt = new HashMap<>();             // key=account<<32|cid → 预定发送时间
    private final Map<Integer, ArrayDeque<Long>> resting = new HashMap<>();
    private final Map<Integer, Integer> nextCid = new HashMap<>();

    Gateway(int id, int[] accounts, String aeronDir, int memberCount, long seed) {
        this.id = id;
        this.accounts = accounts;
        this.rnd = new Random(seed);
        this.cluster = AeronCluster.connect(new AeronCluster.Context()
                .aeronDirectoryName(aeronDir)
                .egressListener(this)
                .ingressChannel("aeron:udp?term-length=1m")
                .ingressEndpoints(ClusterConfig.ingressEndpoints(
                        ExchangeNode.hostnames(memberCount), ExchangeNode.PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                .egressChannel("aeron:udp?endpoint=localhost:0|term-length=1m"));
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
                rejectsByReason[(int) Math.min(reason, 7)]++;
                if (reason != REJECT_UNKNOWN_ORDER && reason != REJECT_DUPLICATE_CANCEL) { // 这两种原因只会来自撤单；其余是下单被拒
                    rejectedPlaces++;
                    if (!record(account, eventCounterOrderId(b, offset))) {
                        late();
                    }
                } else {
                    cancelRejected++;
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

    private void pumpDeposits() {
        for (int i = 0; i < 64 && !depositQueue.isEmpty() && depositsSent - deposited < 500; i++) {
            final long[] d = depositQueue.peekFirst();
            final int len = encodeCommand(buf, 0, CMD_DEPOSIT, (int) d[1], d[0], d[2], d[1], 0); // depositId=资产号，每账户每资产一笔
            if (cluster.offer(buf, 0, len) < 0) {
                backPressure++;
                return;
            }
            depositQueue.pollFirst();
            depositsSent++;
        }
    }

    @Override
    public void run() {
        final IdleStrategy idle = new BackoffIdleStrategy(100, 10, 1_000, 200_000);
        long lastKeepAlive = System.nanoTime();
        int seenPhase = 0;
        long sentThisPhase = 0;
        long baseAccepted = 0;
        long nextSlot = 0;
        long phaseStart = 0;
        while (!stop) {
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
            idle.idle(work);
        }
        cluster.close();
    }

    private boolean sendOne(long intendedNs) {
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
