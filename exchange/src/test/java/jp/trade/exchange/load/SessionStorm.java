package jp.trade.exchange.load;

import static jp.trade.exchange.core.Protocol.*;

import io.aeron.Aeron;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.ClusterConfig;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.LoggingEventSink;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

/**
 * 直接建立 N 个原始集群会话（每个用户一个会话，没有网关复用），看集群在哪里撑不住。
 * 参数：sessions=10000，max=default|<数字>（ConsensusModule.maxConcurrentSessions；default 沿用节点默认值 1000，要看 Aeron 自身默认的 10 请传 -Dmax=10）。
 */
public final class SessionStorm {
    public static void main(String[] args) throws Exception {
        final int sessions = Integer.getInteger("sessions", 10_000);
        final String max = System.getProperty("max", "default");
        System.out.printf("== 原始会话测试：目标 %d 个会话，maxConcurrentSessions=%s%n", sessions, max);

        final AtomicLong nodeErrors = new AtomicLong();
        final File dir = Files.createTempDirectory("exch-sessions").toFile();
        final ExchangeNode node = ExchangeNode.start(0, 1, dir, true, new LoggingEventSink(false), cfg -> {
            if (!"default".equals(max)) {
                cfg.consensusModuleContext().maxConcurrentSessions(Integer.parseInt(max));
            }
            if (Boolean.getBoolean("dedicated")) {
                cfg.mediaDriverContext().threadingMode(io.aeron.driver.ThreadingMode.DEDICATED);
                System.out.println("MediaDriver 线程模型: DEDICATED（conductor/sender/receiver 各一个线程）");
            }
            System.out.println("节点 maxConcurrentSessions=" + cfg.consensusModuleContext().maxConcurrentSessions());
            cfg.errorHandler(t -> {
                if (nodeErrors.incrementAndGet() <= 3) {
                    System.out.println("  [节点错误] " + t);
                }
            });
        });

        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(node.aeronDirectoryName()));
        final long[] responses = {0};
        final EgressListener listener = new EgressListener() {
            @Override
            public void onMessage(long sessionId, long timestamp, DirectBuffer b, int o, int l, Header h) {
                responses[0]++;
            }
        };

        final List<AeronCluster> clients = new ArrayList<>();
        final Map<String, Integer> failures = new LinkedHashMap<>();
        final long t0 = System.nanoTime();
        long lastPrint = t0;
        int attempts = 0;
        try {
            for (int i = 0; i < sessions; i++) {
                attempts++;
                try {
                    clients.add(AeronCluster.connect(new AeronCluster.Context()
                            .aeron(aeron).ownsAeronClient(false)
                            .egressListener(listener)
                            .messageTimeoutNs(TimeUnit.SECONDS.toNanos(3))
                            .ingressChannel("aeron:udp?term-length=64k")
                            .ingressEndpoints(ClusterConfig.ingressEndpoints(
                                    ExchangeNode.hostnames(1), ExchangeNode.PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                            .egressChannel("aeron:udp?endpoint=localhost:0|term-length=64k")));
                } catch (Exception e) {
                    final String key = e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).split("\n")[0];
                    failures.merge(key, 1, Integer::sum);
                    if (failures.values().stream().mapToInt(Integer::intValue).sum() >= 20) {
                        System.out.println("失败已达 20 次，停止继续建立会话");
                        break;
                    }
                }
                if (System.nanoTime() - lastPrint > 5_000_000_000L) {
                    lastPrint = System.nanoTime();
                    System.out.printf("   已建立 %d 个会话，用时 %.0fs，fd=%d%n", clients.size(), (lastPrint - t0) / 1e9, openFds());
                }
            }
            final double connectSecs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("建立会话：尝试 %d，成功 %d，失败 %d，用时 %.1fs（%.1f ms/会话）%n", attempts, clients.size(),
                    failures.values().stream().mapToInt(Integer::intValue).sum(), connectSecs,
                    clients.isEmpty() ? 0 : connectSecs * 1000 / clients.size());
            failures.forEach((k, v) -> System.out.println("   失败原因 ×" + v + "：" + k));
            System.out.printf("资源：打开的 fd=%d，JVM 堆已用=%dMB，aeron 目录=%dMB%n", openFds(),
                    (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20, dirSizeMb(new File(node.aeronDirectoryName())));

            // 每个会话各发一笔入金 + 一笔下单，统计回报
            final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(COMMAND_LENGTH);
            long offered = 0, offerFail = 0;
            final long t1 = System.nanoTime();
            for (int i = 0; i < clients.size(); i++) {
                final AeronCluster c = clients.get(i);
                final long account = 1_000_000 + i;
                int len = encodeCommand(buf, 0, CMD_DEPOSIT, ASSET_QUOTE, account, 1_000_000_000L, 0, 0);
                offered += offerWithRetry(c, buf, len) ? 1 : 0;
                len = encodeCommand(buf, 0, CMD_PLACE, SIDE_BUY, account, 1, 100, 1);
                if (offerWithRetry(c, buf, len)) {
                    offered++;
                } else {
                    offerFail++;
                }
                if (i % 200 == 0) {
                    pollAll(clients);
                }
            }
            final long expect = offered;
            final long deadline = System.currentTimeMillis() + 60_000;
            while (responses[0] < expect && System.currentTimeMillis() < deadline) {
                pollAll(clients);
            }
            System.out.printf("每会话 2 笔命令：已发 %d，收到回报 %d，发送失败 %d，用时 %.1fs%n", offered, responses[0], offerFail,
                    (System.nanoTime() - t1) / 1e9);

            // 保活：10 秒（超过默认的会话超时），看会话是否都还在
            final long keepUntil = System.currentTimeMillis() + 12_000;
            while (System.currentTimeMillis() < keepUntil) {
                for (AeronCluster c : clients) {
                    c.sendKeepAlive();
                }
                pollAll(clients);
                Thread.sleep(500);
            }
            long closed = 0;
            for (AeronCluster c : clients) {
                if (c.isClosed()) {
                    closed++;
                }
            }
            System.out.printf("保活 12s 后：仍在线 %d，已被集群关闭 %d%n", clients.size() - closed, closed);
        } finally {
            System.out.println("集群/驱动错误计数: " + nodeErrors.get());
            for (AeronCluster c : clients) {
                try {
                    c.close();
                } catch (Exception ignored) {
                }
            }
            aeron.close();
            node.close();
        }
        System.exit(0);
    }

    private static boolean offerWithRetry(AeronCluster c, ExpandableArrayBuffer buf, int len) {
        final long deadline = System.currentTimeMillis() + 5_000;
        while (c.offer(buf, 0, len) < 0) {
            if (System.currentTimeMillis() > deadline) {
                return false;
            }
            c.pollEgress();
        }
        return true;
    }

    private static void pollAll(List<AeronCluster> clients) {
        for (AeronCluster c : clients) {
            c.pollEgress();
        }
    }

    private static long openFds() {
        final String[] l = new File("/proc/self/fd").list();
        return l == null ? -1 : l.length;
    }

    private static long dirSizeMb(File d) {
        long total = 0;
        final File[] fs = d.listFiles();
        if (fs != null) {
            for (File f : fs) {
                total += f.isDirectory() ? dirSizeMb(f) << 20 : f.length();
            }
        }
        return total >> 20;
    }
}
