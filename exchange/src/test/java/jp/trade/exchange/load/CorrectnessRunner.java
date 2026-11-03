package jp.trade.exchange.load;

import static jp.trade.exchange.core.Protocol.*;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import jp.trade.exchange.TestDb;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.core.InvariantViolation;
import jp.trade.exchange.core.MatchingEngine;
import jp.trade.exchange.events.LogReplayer;
import jp.trade.exchange.projection.ConnectionFactory;
import jp.trade.exchange.projection.JdbcProjector;
import jp.trade.exchange.projection.ProjectionAuditor;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * 100 万请求的正确性验证（速度无所谓，只证明“不出错”）。
 *
 * 负载：1 万个账户，经 3 个网关会话并发；下单、撤单混合；一部分下单和入金在回报到达后**用同一个键重发**
 * （模拟“回报丢了，超时重试”），重试必须取回原结果、不能重复生效。
 *
 * 验证（全部通过才是 PASS）：
 *  1 服务健康；无序号缺口；无事件被丢弃；无集群错误
 *  2 事件序号严格连续，且等于引擎日志头
 *  3 链式哈希检查点全部核对通过
 *  4 线上状态 == 从日志确定性重放的状态；全量不变量成立；入金没有被重复入账（总供应量精确等于预期）
 *  5 独立参考实现按线上实际应用的命令顺序重新撮合：全部事件逐字段相同，最终余额与在簿订单相同
 *  6 客户端看到的结果与事件流一致；重试得到的“原结果”与原回报一致
 *  7 投影到 MariaDB（带 CHECK 约束）后与引擎状态逐账户逐订单对账一致
 *  8 停机后从日志重放（含 CRC32C 与检查点核对），状态哈希相同，并且仍能继续正确服务
 *
 * 参数：users=10000 ops=1000000 gateways=3 retry=0.05 depositRetry=0.1 window=1000 project=true
 */
public final class CorrectnessRunner {
    static final List<String> FAILURES = new ArrayList<>();

    static boolean check(String name, boolean ok, String detail) {
        System.out.printf("   [%s] %s%s%n", ok ? "通过" : "失败", name, detail.isEmpty() ? "" : " — " + detail);
        if (!ok) {
            FAILURES.add(name);
        }
        return ok;
    }

    private static Evt decode(byte[] e) {
        final UnsafeBuffer b = new UnsafeBuffer(e);
        return new Evt(eventType(b, 0), eventAux(b, 0), eventSeq(b, 0), eventTimestamp(b, 0), eventOrderId(b, 0), eventAccount(b, 0),
                eventPrice(b, 0), eventQty(b, 0), eventCounterOrderId(b, 0), eventCounterAccount(b, 0));
    }

    private record Evt(int type, int aux, long seq, long ts, long orderId, long account, long price, long qty, long cOrder, long cAcct) {
        ReferenceExchange.Evt asRef() { return new ReferenceExchange.Evt(type, aux, seq, ts, orderId, account, price, qty, cOrder, cAcct); }
    }

    public static void main(String[] args) throws Exception {
        final int users = Integer.getInteger("users", 10_000);
        final long ops = Long.getLong("ops", 1_000_000);
        final int gws = Integer.getInteger("gateways", 3);
        final double retry = Double.parseDouble(System.getProperty("retry", "0.05"));
        final double depositRetry = Double.parseDouble(System.getProperty("depositRetry", "0.1"));
        final int window = Integer.getInteger("window", 1_000);
        final boolean project = Boolean.parseBoolean(System.getProperty("project", "true"));

        System.out.printf("== 正确性验证：%d 个账户，%d 条命令（含约 %.0f%% 的下单重试、%.0f%% 的入金重试），%d 个网关会话%n",
                users, ops, retry * 100, depositRetry * 100, gws);

        final AtomicLong nodeErrors = new AtomicLong();
        final LoadRunner.CheckingSink sink = new LoadRunner.CheckingSink();
        final List<byte[]> tap = Collections.synchronizedList(new ArrayList<>((int) (ops * 11 / 10) + 100_000));
        final File dir = Files.createTempDirectory("exch-correct").toFile();
        final ExchangeNode node = ExchangeNode.start(0, 1, dir, true, sink, cfg -> {
            cfg.consensusModuleContext().maxConcurrentSessions(1_000);
            cfg.errorHandler(t -> {
                if (nodeErrors.incrementAndGet() <= 5) {
                    t.printStackTrace();
                }
            });
        }, svc -> svc.fatalHandler(t -> System.out.println("!! FATAL: " + t)).commandTap((ts, cmd) -> {
            final byte[] r = new byte[8 + cmd.length];
            java.nio.ByteBuffer.wrap(r).order(java.nio.ByteOrder.LITTLE_ENDIAN).putLong(ts).put(cmd);
            tap.add(r);
        }));

        final List<Gateway> gateways = new ArrayList<>();
        final List<Thread> threads = new ArrayList<>();
        MatchingEngine truth;
        String liveHash;
        long head;
        try {
            for (int g = 0; g < gws; g++) {
                final int gg = g;
                final int[] mine = java.util.stream.IntStream.rangeClosed(1, users).filter(a -> a % gws == gg).toArray();
                final Gateway gw = new Gateway(g, mine, node.aeronDirectoryName(), 1, 777 + g);
                gw.retryProbability = retry;
                gw.depositRetryProbability = depositRetry;
                gateways.add(gw);
            }
            for (Gateway g : gateways) {
                g.queueDeposits(LoadRunner.QUOTE_DEPOSIT, LoadRunner.BASE_DEPOSIT);
            }
            for (Gateway g : gateways) {
                final Thread t = new Thread(g, "gateway-" + g.id);
                t.setUncaughtExceptionHandler((th, e) -> System.out.println("!! " + th.getName() + " died: " + e));
                threads.add(t);
                t.start();
            }
            LoadRunner.awaitDeposits(gateways, 2L * users, 120_000, node);
            LoadRunner.runPhase("满载 " + ops + " 条命令", gateways, ops / gws, 0, window, 1_800);
            Thread.sleep(3_000); // 让事件发布线程、检查点消息都落地

            // ======== 验证 ========
            System.out.println("\n== 验证");
            long retriesSent = 0, dupChecked = 0, dupMismatch = 0, dupLearned = 0, depDup = 0, depMismatch = 0, lost = 0;
            for (Gateway g : gateways) {
                retriesSent += g.retriesSent;
                dupChecked += g.dupChecked;
                dupMismatch += g.dupMismatch;
                dupLearned += g.dupLearned;
                depDup += g.depositDupChecked;
                depMismatch += g.depositDupMismatch;
                lost += g.lostPlaces;
            }
            System.out.printf("   负载实际：重试下单 %d 笔（得到“原结果”回复 %d），重试入金 %d 笔，丢失回报 %d 笔%n", retriesSent, dupChecked, depDup, lost);

            // 1 服务健康
            check("1 服务健康、无丢弃、无集群错误",
                    !node.service().halted() && node.service().droppedEvents() == 0 && node.bridge().deduper().gaps() == 0
                            && sink.gaps == 0 && nodeErrors.get() == 0,
                    "halted=" + node.service().halted() + " 事件丢弃=" + node.service().droppedEvents() + " 序号缺口=" + sink.gaps
                            + " 错误=" + nodeErrors.get() + " egress回报丢弃(可由重试取回)=" + node.service().egressDropped());

            // 4 线上状态 vs 日志重放，及全量不变量
            final long tReplay = System.nanoTime();
            try (LogReplayer r = new LogReplayer(node.clusterDir(), node.aeronDirectoryName(), node.archiveControlEndpoint())) {
                truth = r.rebuildState();
                liveHash = node.service().stateHash();
                check("4a 线上状态 == 从日志确定性重放的状态",
                        liveHash.equals(truth.stateHash()), String.format("重放 %d 个事件用时 %.1fs", truth.lastEventSeq(), (System.nanoTime() - tReplay) / 1e9));
                check("3 日志重放时核对的检查点全部通过", r.checkpointsVerified() > 0, "核对 " + r.checkpointsVerified() + " 个");
            }
            head = truth.lastEventSeq();
            try {
                truth.verifyInvariants();
                check("4b 全量不变量（余额非负、守恒、冻结额==在簿订单所需）", true, "在簿订单 " + truth.restingOrderCount());
            } catch (InvariantViolation v) {
                check("4b 全量不变量", false, v.getMessage());
            }
            check("4c 入金没有被重复入账（总供应量精确等于预期）",
                    truth.totalHeld(ASSET_QUOTE) == (long) users * LoadRunner.QUOTE_DEPOSIT
                            && truth.totalHeld(ASSET_BASE) == (long) users * LoadRunner.BASE_DEPOSIT,
                    "QUOTE=" + truth.totalHeld(ASSET_QUOTE) + " BASE=" + truth.totalHeld(ASSET_BASE));
            final long expectedCheckpoints = head / MatchingEngine.CHECKPOINT_INTERVAL;
            check("3 在线检查点全部核对通过",
                    node.service().checkpointsVerified() == expectedCheckpoints && node.service().checkpointsUnverifiable() == 0
                            && node.service().checkpointsFailed() == 0,
                    "核对 " + node.service().checkpointsVerified() + " / 期望 " + expectedCheckpoints
                            + "，写入日志失败 " + node.service().checkpointsFailed());

            // 2 事件序号连续
            boolean contiguous = sink.events.size() == head;
            long badAt = -1;
            final List<byte[]> events = new ArrayList<>(sink.events);
            for (int i = 0; i < events.size() && contiguous; i++) {
                if (eventSeq(new UnsafeBuffer(events.get(i)), 0) != i + 1) {
                    contiguous = false;
                    badAt = i + 1;
                }
            }
            check("2 事件序号 1.." + head + " 严格连续，且数量等于引擎日志头", contiguous, "下游收到 " + events.size() + (badAt > 0 ? "，错位于 " + badAt : ""));

            // 5 独立参考实现差分验证
            final long tRef = System.nanoTime();
            final ReferenceExchange ref = new ReferenceExchange();
            final List<byte[]> commands = new ArrayList<>(tap);
            for (byte[] c : commands) {
                final java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(c).order(java.nio.ByteOrder.LITTLE_ENDIAN);
                ref.apply(bb.getLong(0), java.util.Arrays.copyOfRange(c, 8, c.length));
            }
            boolean same = ref.events.size() == events.size();
            String firstDiff = "";
            for (int i = 0; i < Math.min(ref.events.size(), events.size()) && same; i++) {
                if (!ref.events.get(i).equals(decode(events.get(i)).asRef())) {
                    same = false;
                    firstDiff = "第 " + (i + 1) + " 个事件：参考=" + ref.events.get(i) + " 实际=" + decode(events.get(i));
                }
            }
            check("5a 独立参考实现重新撮合 " + commands.size() + " 条命令，" + ref.events.size() + " 个事件与线上逐字段完全相同", same,
                    same ? String.format("用时 %.1fs", (System.nanoTime() - tRef) / 1e9) : firstDiff + "（事件数 参考=" + ref.events.size() + " 实际=" + events.size() + "）");
            final int[] accountDiffs = {0};
            final String[] firstAcct = {""};
            truth.forEachAccount((acct, ab, aq, fb, fq) -> {
                final long[] av = ref.avail.getOrDefault(acct, new long[3]);
                final long[] fr = ref.frozen.getOrDefault(acct, new long[3]);
                if (av[ASSET_BASE] != ab || av[ASSET_QUOTE] != aq || fr[ASSET_BASE] != fb || fr[ASSET_QUOTE] != fq) {
                    if (accountDiffs[0]++ == 0) {
                        firstAcct[0] = "账户 " + acct + " 引擎=[" + ab + "," + aq + "," + fb + "," + fq + "] 参考=[" + av[1] + "," + av[2] + "," + fr[1] + "," + fr[2] + "]";
                    }
                }
            });
            final Map<Long, long[]> refOrders = ref.openOrders();
            final int[] orderDiffs = {0};
            truth.forEachOpenOrder((id, acct, side, price, remaining) -> {
                final long[] o = refOrders.get(id);
                if (o == null || o[0] != acct || o[1] != side || o[2] != price || o[3] != remaining) {
                    orderDiffs[0]++;
                }
            });
            check("5b 最终全部账户余额与参考实现一致", accountDiffs[0] == 0, accountDiffs[0] == 0 ? "" : accountDiffs[0] + " 个账户不一致，如 " + firstAcct[0]);
            check("5c 在簿订单与参考实现一致", orderDiffs[0] == 0 && refOrders.size() == truth.restingOrderCount(),
                    "引擎 " + truth.restingOrderCount() + " / 参考 " + refOrders.size() + "，不一致 " + orderDiffs[0]);

            // 6 客户端视角 == 事件流
            final Map<Long, Long> truthOutcome = new HashMap<>();
            for (byte[] e : events) {
                final UnsafeBuffer b = new UnsafeBuffer(e);
                final int t = eventType(b, 0);
                if (t == EVT_ACCEPTED) {
                    truthOutcome.put((eventAccount(b, 0) << 32) | eventCounterOrderId(b, 0), eventOrderId(b, 0));
                } else if (t == EVT_REJECTED) {
                    final long reason = eventQty(b, 0);
                    if (reason != REJECT_UNKNOWN_ORDER && reason != REJECT_DUPLICATE_CANCEL && reason != REJECT_DUPLICATE_DEPOSIT
                            && reason != REJECT_DUPLICATE_CLIENT_ORDER) {
                        truthOutcome.put((eventAccount(b, 0) << 32) | eventCounterOrderId(b, 0), -reason);
                    }
                }
            }
            long viewChecked = 0, viewMismatch = 0;
            for (Gateway g : gateways) {
                for (Map.Entry<Long, Long> e : g.observed.entrySet()) {
                    viewChecked++;
                    if (!e.getValue().equals(truthOutcome.get(e.getKey()))) {
                        viewMismatch++;
                    }
                }
            }
            check("6a 客户端看到的每个结果（" + viewChecked + " 个）都与事件流一致", viewMismatch == 0, "不一致 " + viewMismatch);
            check("6b 下单重试取回的“原结果”与原回报一致（共 " + dupChecked + " 次，其中回报丢失后靠重试取回 " + dupLearned + " 次）",
                    dupMismatch == 0 && dupChecked > 0, "不一致 " + dupMismatch);
            check("6c 入金重试返回原金额、没有重复入账（共 " + depDup + " 次）", depMismatch == 0 && depDup > 0, "不一致 " + depMismatch);

            // 7 投影到 MariaDB
            if (project) {
                final long tProj = System.nanoTime();
                final ConnectionFactory db = TestDb.freshSchema();
                final JdbcProjector projector = new JdbcProjector(db);
                boolean projected = true;
                String projErr = "";
                try {
                    for (int i = 0; i < events.size(); i += 1_000) {
                        projector.apply(new ArrayList<>(events.subList(i, Math.min(events.size(), i + 1_000))));
                    }
                } catch (RuntimeException e) {
                    projected = false;
                    projErr = e.toString();
                }
                if (projected) {
                    try (Connection c = db.get()) {
                        final ProjectionAuditor.Result r = ProjectionAuditor.audit(c, truth);
                        check("7 投影到 MariaDB（含 CHECK 约束）后与引擎逐账户逐订单对账一致", r.clean(),
                                String.format("用时 %.0fs", (System.nanoTime() - tProj) / 1e9) + (r.clean() ? "" : " " + r.diffs().subList(0, Math.min(3, r.diffs().size()))));
                    }
                } else {
                    check("7 投影到 MariaDB", false, projErr);
                }
            }
        } finally {
            for (Gateway g : gateways) {
                g.stop = true;
            }
            for (Thread t : threads) {
                t.join(5_000);
            }
        }

        // 8 停机后从日志重放
        System.out.println("\n== 停机，从日志重放并继续服务");
        node.close();
        final long t8 = System.nanoTime();
        final LoadRunner.CheckingSink sink2 = new LoadRunner.CheckingSink();
        try (ExchangeNode n2 = ExchangeNode.start(0, 1, dir, false, sink2, cfg -> {}, svc -> svc.fatalHandler(t -> System.out.println("!! FATAL after restart: " + t)))) {
            final long deadline = System.currentTimeMillis() + 900_000;
            String h2 = "";
            while (System.currentTimeMillis() < deadline && !n2.service().halted()) {
                try {
                    h2 = n2.service().stateHash();
                } catch (RuntimeException racy) {
                    h2 = "";
                }
                if (liveHash.equals(h2)) {
                    break;
                }
                Thread.sleep(500);
            }
            check("8a 重启后从日志重放（CRC32C + 检查点核对），状态哈希与停机前相同", liveHash.equals(h2) && !n2.service().halted(),
                    String.format("用时 %.1fs，重放中核对检查点 %d 个", (System.nanoTime() - t8) / 1e9, n2.service().checkpointsVerified()));
            try (ExchangeClient c = new ExchangeClient(n2.aeronDirectoryName())) {
                c.deposit(users + 1, ASSET_QUOTE, 1_000, 424242);
                final byte[] resp = c.await(1, 30_000).get(0);
                check("8b 重启后继续正确服务", eventType(new UnsafeBuffer(resp), 0) == EVT_DEPOSITED, "");
            } catch (RuntimeException e) {
                check("8b 重启后继续正确服务", false, e.toString());
            }
        }

        System.out.println();
        System.out.println(FAILURES.isEmpty() ? "RESULT: PASS —— " + ops + " 条命令下没有发现任何错误" : "RESULT: FAIL —— " + FAILURES);
        System.exit(FAILURES.isEmpty() ? 0 : 1);
    }
}
