package jp.trade.exchange.load;

import static jp.trade.exchange.core.Protocol.*;

import com.sun.management.OperatingSystemMXBean;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import jp.trade.exchange.TestDb;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.core.MatchingEngine;
import jp.trade.exchange.events.EventSink;
import jp.trade.exchange.events.LogReplayer;
import jp.trade.exchange.projection.ConnectionFactory;
import jp.trade.exchange.projection.JdbcProjector;
import jp.trade.exchange.projection.ProjectionAuditor;
import jp.trade.exchange.projection.ProjectionPipeline;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * 1 万活跃账户的并发压测：账户请求经少量网关会话复用进集群（设计 §1），
 * 先限速跑一段看延迟，再不限速跑到饱和看吞吐，最后做全量一致性校验。
 *
 * 参数（-D）：users=10000 gateways=3 pacedRate=5000 pacedSeconds=15 saturationOps=100000 window=2000 project=true
 */
public final class LoadRunner {
    static final long QUOTE_DEPOSIT = 10_000_000_000_000L;
    static final long BASE_DEPOSIT = 1_000_000_000L;

    /** 保存全部事件并检查序号连续。 */
    static final class CheckingSink implements EventSink {
        final List<byte[]> events = Collections.synchronizedList(new ArrayList<>(1 << 20));
        volatile long gaps;
        private long expected = 1;

        @Override
        public void publish(long seq, byte[] event) {
            if (seq != expected) {
                gaps++;
            }
            expected = seq + 1;
            events.add(event);
        }
    }

    public static void main(String[] args) throws Exception {
        final int users = Integer.getInteger("users", 10_000);
        final int gws = Integer.getInteger("gateways", 3);
        final double pacedRate = Double.parseDouble(System.getProperty("pacedRate", "5000"));
        final int pacedSeconds = Integer.getInteger("pacedSeconds", 15);
        final long satOps = Long.getLong("saturationOps", 100_000);
        final int window = Integer.getInteger("window", 2_000);
        final boolean project = Boolean.parseBoolean(System.getProperty("project", "true"));

        System.out.printf("== 负载测试：%d 个活跃账户，%d 个网关会话，CPU 核数=%d%n", users, gws, Runtime.getRuntime().availableProcessors());
        final AtomicLong nodeErrors = new AtomicLong();
        final CheckingSink sink = new CheckingSink();
        final File dir = Files.createTempDirectory("exch-load").toFile();
        final ExchangeNode node = ExchangeNode.start(0, 1, dir, true, sink, cfg -> {
            cfg.consensusModuleContext().maxConcurrentSessions(1_000);
            final int sync = Integer.getInteger("sync", -1); // 0=不 fsync 1=fdatasync 2=fsync；-1 沿用节点默认
            if (sync >= 0) {
                cfg.archiveContext().fileSyncLevel(sync).catalogFileSyncLevel(sync);
            }
            cfg.errorHandler(t -> {
                if (nodeErrors.incrementAndGet() <= 5) {
                    t.printStackTrace();
                }
            });
        });

        final List<Gateway> gateways = new ArrayList<>();
        final List<Thread> threads = new ArrayList<>();
        try {
            // 账户 1..users 轮流分给各网关
            for (int g = 0; g < gws; g++) {
                final int gg = g;
                final int[] mine = java.util.stream.IntStream.rangeClosed(1, users).filter(a -> a % gws == gg).toArray();
                gateways.add(new Gateway(g, mine, node.aeronDirectoryName(), 1, 1234 + g));
            }
            // ---- 开户 ----
            final long t0 = System.nanoTime();
            for (Gateway g : gateways) {
                g.queueDeposits(QUOTE_DEPOSIT, BASE_DEPOSIT);
            }
            for (Gateway g : gateways) {
                final Thread t = new Thread(g, "gateway-" + g.id);
                t.setUncaughtExceptionHandler((th, e) -> {
                    System.out.println("!! " + th.getName() + " died: " + e);
                    e.printStackTrace(System.out);
                });
                threads.add(t);
                t.start();
            }
            awaitDeposits(gateways, 2L * users, 60_000, node);
            System.out.printf("开户完成：%d 笔入金，用时 %.1fs%n", 2L * users, (System.nanoTime() - t0) / 1e9);

            // ---- 阶段 1：限速（看延迟）----
            runPhase("阶段1 限速 " + (long) pacedRate + " ops/s × " + pacedSeconds + "s", gateways,
                    (long) (pacedRate * pacedSeconds / gws), pacedRate / gws, window, pacedSeconds + 90);
            // ---- 阶段 2：不限速（看饱和吞吐）----
            runPhase("阶段2 饱和（窗口 " + window + "/网关）", gateways, satOps / gws, 0, window, 600);

            // ---- 校验 ----
            System.out.println("== 一致性校验");
            Thread.sleep(2_000); // 等事件发布线程刷出
            verify(node, sink, users, project);
        } finally {
            for (Gateway g : gateways) {
                g.stop = true;
            }
            for (Thread t : threads) {
                t.join(5_000);
            }
            node.close();
            System.out.println("集群/驱动错误计数: " + nodeErrors.get());
        }
        System.exit(0);
    }

    static void awaitDeposits(List<Gateway> gws, long expected, long timeoutMs, ExchangeNode node) throws Exception {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            long d = 0;
            for (Gateway g : gws) {
                d += g.deposited;
            }
            if (d >= expected) {
                return;
            }
            Thread.sleep(50);
        }
        for (Gateway g : gws) {
            System.out.printf("   gateway %d: 已发 %d 笔入金，已确认 %d，背压 %d%n", g.id, g.depositsSent, g.deposited, g.backPressure);
        }
        System.out.printf("   服务端 egress 因背压丢弃=%d，事件发布丢弃=%d%n", node.service().egressDropped(), node.service().droppedEvents());
        throw new IllegalStateException("deposits not acknowledged in time");
    }

    static void runPhase(String name, List<Gateway> gws, long opsPerGateway, double ratePerGateway, int window,
                         long timeoutSec) throws Exception {
        System.out.println("\n-- " + name);
        for (Gateway g : gws) {
            g.resetStats();
        }
        final OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        os.getProcessCpuLoad();
        final long t0 = System.nanoTime();
        for (Gateway g : gws) {
            g.startPhase(opsPerGateway, ratePerGateway, window);
        }
        final long deadline = System.currentTimeMillis() + timeoutSec * 1000;
        long lastPrint = 0;
        while (true) {
            boolean all = true;
            long acked = 0;
            for (Gateway g : gws) {
                all &= g.phaseDone();
                acked += g.accepted + g.rejectedPlaces;
            }
            if (all) {
                break;
            }
            if (System.currentTimeMillis() > deadline) {
                System.out.println("!! 阶段超时，部分请求未得到回报");
                break;
            }
            if (System.currentTimeMillis() - lastPrint > 5_000) {
                lastPrint = System.currentTimeMillis();
                System.out.printf("   进度 acked=%d%n", acked);
            }
            Thread.sleep(100);
        }
        final double secs = (System.nanoTime() - t0) / 1e9;
        final double cpu = os.getProcessCpuLoad() * 100;

        long lost = 0, late = 0, sent = 0, accepted = 0, rejected = 0, cancels = 0, cancelled = 0, cancelRej = 0, trades = 0, bp = 0, nc = 0, aa = 0, of = 0;
        final long[] reasons = new long[8];
        int n = 0;
        for (Gateway g : gws) {
            lost += g.lostPlaces;
            late += g.lateResponses;
            sent += g.placesSent;
            accepted += g.accepted;
            rejected += g.rejectedPlaces;
            cancels += g.cancelsSent;
            cancelled += g.cancelled;
            cancelRej += g.cancelRejected;
            trades += g.trades;
            bp += g.backPressure;
            nc += g.notConnected;
            aa += g.adminAction;
            of += g.otherOfferFail;
            n += g.latCount;
            for (int i = 0; i < 8; i++) {
                reasons[i] += g.rejectsByReason[i];
            }
        }
        final long[] all = new long[n];
        int k = 0;
        for (Gateway g : gws) {
            System.arraycopy(g.latencies, 0, all, k, g.latCount);
            k += g.latCount;
        }
        Arrays.sort(all);
        System.out.printf("   下单 %d（ACCEPTED %d，REJECTED %d），撤单 %d（成功 %d，被拒 %d），成交回报 %d%n",
                sent, accepted, rejected, cancels, cancelled, cancelRej, trades);
        System.out.printf("   用时 %.1fs  吞吐 %.0f 命令/s  进程 CPU 占用 %.0f%%（总共 %d 核）%n",
                secs, (sent + cancels) / secs, cpu, Runtime.getRuntime().availableProcessors());
        if (n > 0) {
            System.out.printf("   下单延迟(含排队)  p50=%s p90=%s p99=%s p99.9=%s max=%s%n",
                    fmt(all[(int) (n * 0.50)]), fmt(all[(int) (n * 0.90)]), fmt(all[(int) (n * 0.99)]),
                    fmt(all[(int) Math.min(n - 1, n * 0.999)]), fmt(all[n - 1]));
        }
        System.out.printf("   发送失败: 背压 %d, 未连接 %d, 管理动作 %d, 其他 %d；拒绝原因 [资金不足=%d 重复=%d 无效=%d 未知订单=%d 溢出=%d]%n",
                bp, nc, aa, of, reasons[REJECT_INSUFFICIENT_FUNDS], reasons[REJECT_DUPLICATE_CLIENT_ORDER],
                reasons[REJECT_INVALID], reasons[REJECT_UNKNOWN_ORDER], reasons[REJECT_OVERFLOW]);
        if (lost > 0 || late > 0) {
            System.out.printf("!! 回报丢失: 判定丢失后仍未到 %d 笔，判定丢失后又晚到 %d 笔%n", lost, late);
        }
    }

    static String fmt(long nanos) {
        return nanos < 1_000_000 ? String.format("%.0fµs", nanos / 1e3) : String.format("%.1fms", nanos / 1e6);
    }

    static void verify(ExchangeNode node, CheckingSink sink, int users, boolean project) throws Exception {
        final long tReplay = System.nanoTime();
        MatchingEngine truth;
        try (LogReplayer r = new LogReplayer(node.clusterDir(), node.aeronDirectoryName(), node.archiveControlEndpoint())) {
            truth = r.rebuildState();
        }
        final double replaySecs = (System.nanoTime() - tReplay) / 1e9;
        final String live = node.service().stateHash();
        final String replayed = truth.stateHash();
        System.out.printf("egress 回报因背压丢弃=%d%n", node.service().egressDropped());
        System.out.printf("事件总数（引擎）=%d，发布到下游=%d，序号缺口=%d，IPC 背压丢弃=%d，桥内去重缺口=%d%n",
                truth.lastEventSeq(), sink.events.size(), sink.gaps, node.service().droppedEvents(),
                node.bridge().deduper().gaps());
        System.out.printf("日志确定性重放 %d 条事件用时 %.1fs；状态哈希一致=%s%n", truth.lastEventSeq(), replaySecs, live.equals(replayed));
        System.out.printf("资产守恒：QUOTE %s，BASE %s%n",
                truth.totalHeld(ASSET_QUOTE) == users * LoadRunner.QUOTE_DEPOSIT ? "OK" : "FAIL(" + truth.totalHeld(ASSET_QUOTE) + ")",
                truth.totalHeld(ASSET_BASE) == users * LoadRunner.BASE_DEPOSIT ? "OK" : "FAIL(" + truth.totalHeld(ASSET_BASE) + ")");
        System.out.printf("在簿订单数=%d%n", truth.restingOrderCount());

        if (project) {
            System.out.println("\n== 投影到 MariaDB（单线程、批量事务）");
            final ConnectionFactory db = TestDb.freshSchema();
            final ProjectionPipeline p = new ProjectionPipeline(new JdbcProjector(db), null, 1_000);
            final long t0 = System.nanoTime();
            final List<byte[]> evs = new ArrayList<>(sink.events);
            for (byte[] e : evs) {
                p.publish(eventSeq(new UnsafeBuffer(e), 0), e);
            }
            p.flush();
            final double secs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("投影 %d 条事件用时 %.1fs（%.0f 事件/s）%n", evs.size(), secs, evs.size() / secs);
            try (Connection c = db.get()) {
                final ProjectionAuditor.Result r = ProjectionAuditor.audit(c, truth);
                System.out.printf("对账：%s%s%n", r.clean() ? "一致" : "不一致", r.clean() ? "" : " " + r.diffs().subList(0, Math.min(5, r.diffs().size())));
            }
        }
    }
}
