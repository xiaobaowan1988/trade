package jp.trade.exchange.load;

import io.aeron.Publication;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.checksum.Checksums;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.ClusterConfig;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;

/**
 * 定序器单独压测：把撮合换成空服务（{@link NoopService}），只测 Aeron Cluster 的定序
 * （接入 → Raft 日志 → fsync → 多数派确认 → 交付）。慢无所谓，重点是“不出错”：
 * 序号连续（不丢不重不乱序）、各副本全局顺序哈希一致、停机后从日志重放哈希一致。
 *
 * 参数（-D）：messages=1000000 gateways=3 members=1 sync=2 crc=true driver=shared archive=shared
 *            acks=1000（每多少条回一个确认用于采样提交延迟）
 */
public final class SequencerBench {
    private static final String HOST = "localhost";
    private static final int PORT_BASE = 9000;

    record Node(int id, File dir, ClusteredMediaDriver driver, ClusteredServiceContainer container, NoopService service) {
        void close() {
            CloseHelper.quietClose(container);
            CloseHelper.quietClose(driver);
        }

        String aeronDir() { return driver.mediaDriver().aeronDirectoryName(); }
    }

    static Node start(int id, int members, File dir, boolean fresh, int sync, boolean crc, String driverMode, String archiveMode, int ackEvery) {
        if (fresh) {
            IoUtil.delete(dir, true);
        }
        dir.mkdirs();
        final NoopService service = new NoopService(ackEvery);
        final List<String> hosts = new ArrayList<>();
        for (int i = 0; i < members; i++) {
            hosts.add(HOST);
        }
        final ClusterConfig cfg = ClusterConfig.create(id, hosts, PORT_BASE, service);
        cfg.baseDir(dir);
        cfg.aeronDirectoryName(new File(dir, "aeron").getAbsolutePath());
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.mediaDriverContext().threadingMode("dedicated".equals(driverMode) ? ThreadingMode.DEDICATED : ThreadingMode.SHARED).dirDeleteOnStart(true);
        cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=1m")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(200))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(4))
                .maxConcurrentSessions(100);
        cfg.archiveContext()
                .deleteArchiveOnStart(false)
                .threadingMode("dedicated".equals(archiveMode) ? ArchiveThreadingMode.DEDICATED : ArchiveThreadingMode.SHARED)
                .fileSyncLevel(sync).catalogFileSyncLevel(sync);
        if (crc) {
            cfg.archiveContext().recordChecksum(Checksums.crc32c()).replayChecksum(Checksums.crc32c());
        }
        final ClusteredMediaDriver driver = ClusteredMediaDriver.launch(
                cfg.mediaDriverContext(), cfg.archiveContext(), cfg.consensusModuleContext());
        final ClusteredServiceContainer container = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());
        return new Node(id, dir, driver, container, service);
    }

    /** 一个发送方：独立线程、独立集群会话，只负责尽快、按序提交。 */
    static final class Sender implements Runnable, EgressListener {
        final long id;
        final long target;
        final AeronCluster cluster;
        final int ackEvery;
        volatile long offered;
        volatile boolean done;
        volatile Throwable error;
        long backPressured, adminAction, notConnected, other;
        final long[] latencies;
        int latCount;
        private final java.util.HashMap<Long, Long> sentAt = new java.util.HashMap<>();

        Sender(long id, long target, String aeronDir, int members, int ackEvery) {
            this.id = id;
            this.target = target;
            this.ackEvery = ackEvery;
            this.latencies = new long[ackEvery > 0 ? (int) (target / ackEvery) + 8 : 0];
            final List<String> hosts = new ArrayList<>();
            for (int i = 0; i < members; i++) {
                hosts.add(HOST);
            }
            this.cluster = AeronCluster.connect(new AeronCluster.Context()
                    .aeronDirectoryName(aeronDir)
                    .egressListener(this)
                    .ingressChannel("aeron:udp?term-length=1m")
                    .ingressEndpoints(ClusterConfig.ingressEndpoints(hosts, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                    .egressChannel("aeron:udp?endpoint=localhost:0|term-length=64k"));
        }

        @Override
        public void onMessage(long sessionId, long timestamp, DirectBuffer b, int o, int l, Header h) {
            final Long t0 = sentAt.remove(b.getLong(o + 8));
            if (t0 != null && latCount < latencies.length) {
                latencies[latCount++] = System.nanoTime() - t0;
            }
        }

        @Override
        public void run() {
            try {
                final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(NoopService.MSG_LENGTH);
                buf.setMemory(0, NoopService.MSG_LENGTH, (byte) 0x5A);
                buf.putInt(0, NoopService.MAGIC);
                buf.putLong(8, id);
                final BackoffIdleStrategy idle = new BackoffIdleStrategy(1, 1, 1_000, 100_000);
                long seq = 0;
                long lastKeepAlive = System.nanoTime();
                while (seq < target) {
                    buf.putLong(16, seq + 1);
                    final long r = cluster.offer(buf, 0, NoopService.MSG_LENGTH);
                    if (r >= 0) {
                        seq++;
                        offered = seq;
                        if (ackEvery > 0 && seq % ackEvery == 0) {
                            sentAt.put(seq, System.nanoTime());
                        }
                        if ((seq & 63) == 0) {
                            cluster.pollEgress();
                        }
                        continue;
                    }
                    if (r == Publication.BACK_PRESSURED) {
                        backPressured++;
                    } else if (r == Publication.ADMIN_ACTION) {
                        adminAction++;
                    } else if (r == Publication.NOT_CONNECTED) {
                        notConnected++;
                    } else if (r == Publication.CLOSED || r == Publication.MAX_POSITION_EXCEEDED) {
                        throw new IllegalStateException("offer failed: " + r);
                    } else {
                        other++;
                    }
                    cluster.pollEgress();
                    idle.idle();
                    if (System.nanoTime() - lastKeepAlive > 1_000_000_000L) {
                        cluster.sendKeepAlive();
                        lastKeepAlive = System.nanoTime();
                    }
                }
                done = true;
                // 继续收完剩余的采样确认
                final long end = System.currentTimeMillis() + 5_000;
                while (latCount < latencies.length - 8 && System.currentTimeMillis() < end) {
                    cluster.pollEgress();
                    cluster.sendKeepAlive();
                    Thread.sleep(5);
                }
            } catch (Throwable t) {
                error = t;
                done = true;
            }
        }

        void close() { cluster.close(); }
    }

    public static void main(String[] args) throws Exception {
        final long messages = Long.getLong("messages", 1_000_000);
        final int gateways = Integer.getInteger("gateways", 3);
        final int members = Integer.getInteger("members", 1);
        final int sync = Integer.getInteger("sync", 2);
        final boolean crc = Boolean.parseBoolean(System.getProperty("crc", "true"));
        final String driverMode = System.getProperty("driver", "shared");
        final String archiveMode = System.getProperty("archive", "shared");
        final int ackEvery = Integer.getInteger("acks", 1000);
        final long perGateway = messages / gateways;
        final long total = perGateway * gateways;

        System.out.printf("== 定序器单独压测：%d 条消息（每条 %dB），%d 个发送方，%d 个成员，fsync=%d，CRC32C=%s，driver=%s，archive=%s，%d 核%n",
                total, NoopService.MSG_LENGTH, gateways, members, sync, crc, driverMode, archiveMode, Runtime.getRuntime().availableProcessors());

        final File base = Files.createTempDirectory("seqbench").toFile();
        final Node[] nodes = new Node[members];
        for (int i = 0; i < members; i++) {
            nodes[i] = start(i, members, new File(base, "n" + i), true, sync, crc, driverMode, archiveMode, ackEvery);
        }
        boolean ok = true;
        try {
            // 等 leader
            final long ld = System.currentTimeMillis() + 60_000;
            while (java.util.Arrays.stream(nodes).noneMatch(n -> n.service().role == Cluster.Role.LEADER)) {
                if (System.currentTimeMillis() > ld) {
                    throw new IllegalStateException("no leader");
                }
                Thread.sleep(50);
            }
            final String clientDir = Arrays.stream(nodes).filter(n -> n.service().role == Cluster.Role.LEADER).findFirst().get().aeronDir();

            final List<Sender> senders = new ArrayList<>();
            final List<Thread> threads = new ArrayList<>();
            for (int g = 1; g <= gateways; g++) {
                senders.add(new Sender(g, perGateway, clientDir, members, ackEvery));
            }
            final long t0 = System.nanoTime();
            for (Sender s : senders) {
                final Thread t = new Thread(s, "sender-" + s.id);
                threads.add(t);
                t.start();
            }
            long offeredDoneAt = 0;
            long lastPrint = t0;
            final long deadline = System.currentTimeMillis() + 3_600_000; // 慢无所谓
            while (true) {
                long done = 0;
                boolean allDone = true;
                for (Sender s : senders) {
                    done += s.offered;
                    allDone &= s.done;
                    if (s.error != null) {
                        throw new IllegalStateException("sender failed", s.error);
                    }
                }
                if (allDone && offeredDoneAt == 0) {
                    offeredDoneAt = System.nanoTime();
                }
                final boolean allApplied = Arrays.stream(nodes).allMatch(n -> n.service().count >= total);
                if (allApplied) {
                    break;
                }
                for (Node n : nodes) {
                    if (n.service().halted) {
                        throw new IllegalStateException("service halted: " + n.service().firstViolation);
                    }
                }
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("timeout");
                }
                if (System.nanoTime() - lastPrint > 5_000_000_000L) {
                    lastPrint = System.nanoTime();
                    System.out.printf("   已提交 %d / 已交付(成员0) %d%n", done, nodes[0].service().count);
                }
                Thread.sleep(20);
            }
            final long t1 = System.nanoTime();
            for (Thread t : threads) {
                t.join(10_000);
            }
            final double secs = (t1 - t0) / 1e9;
            System.out.printf("定序 %d 条消息用时 %.2fs → %.0f 条/s（约 %.1f MB/s 日志，不含帧头对齐前的负载）%n", total, secs, total / secs,
                    total * 128.0 / secs / 1e6);
            if (offeredDoneAt != 0) {
                System.out.printf("   发送方全部提交完用时 %.2fs；之后到全部副本交付又过了 %.2fs%n", (offeredDoneAt - t0) / 1e9, (t1 - offeredDoneAt) / 1e9);
            }
            long bp = 0, aa = 0, nc = 0, ot = 0;
            final List<Long> lat = new ArrayList<>();
            for (Sender s : senders) {
                bp += s.backPressured;
                aa += s.adminAction;
                nc += s.notConnected;
                ot += s.other;
                for (int i = 0; i < s.latCount; i++) {
                    lat.add(s.latencies[i]);
                }
            }
            System.out.printf("   发送端遇到：背压 %d，管理动作 %d，未连接 %d，其他 %d%n", bp, aa, nc, ot);
            if (!lat.isEmpty()) {
                java.util.Collections.sort(lat);
                final int n = lat.size();
                System.out.printf("   提交延迟（采样 %d 个，含排队）p50=%s p99=%s max=%s%n", n, fmt(lat.get(n / 2)), fmt(lat.get((int) (n * 0.99))), fmt(lat.get(n - 1)));
            }

            // ---- 正确性：交付数、序号连续、副本一致 ----
            Thread.sleep(500);
            System.out.println("-- 正确性校验（运行中）");
            ok &= verifyLive(nodes, total);

            for (Sender s : senders) {
                s.close();
            }

            // ---- 正确性：停机后从日志重放，全局顺序哈希必须相同 ----
            final long[] liveChain = Arrays.stream(nodes).mapToLong(n -> n.service().chain).toArray();
            for (Node n : nodes) {
                n.close();
            }
            System.out.println("-- 停机，从日志重放（Archive 回放，CRC32C=" + crc + "）");
            final long r0 = System.nanoTime();
            for (int i = 0; i < members; i++) {
                nodes[i] = start(i, members, new File(base, "n" + i), false, sync, crc, driverMode, archiveMode, ackEvery);
            }
            final long rd = System.currentTimeMillis() + 600_000;
            while (!Arrays.stream(nodes).allMatch(n -> n.service().count >= total)) {
                for (Node n : nodes) {
                    if (n.service().halted) {
                        throw new IllegalStateException("replay halted: " + n.service().firstViolation);
                    }
                }
                if (System.currentTimeMillis() > rd) {
                    throw new IllegalStateException("replay timeout, counts=" + Arrays.stream(nodes).mapToLong(n -> n.service().count).boxed().toList());
                }
                Thread.sleep(50);
            }
            System.out.printf("   重放 %d 条用时 %.1fs%n", total, (System.nanoTime() - r0) / 1e9);
            for (int i = 0; i < members; i++) {
                final boolean same = nodes[i].service().chain == liveChain[i];
                System.out.printf("   成员 %d：重放哈希 %s 运行时哈希 → %s%n", i, same ? "==" : "!=", same ? "一致" : "不一致");
                ok &= same;
                ok &= nodes[i].service().count == total && nodes[i].service().violations == 0;
            }
        } finally {
            for (Node n : nodes) {
                if (n != null) {
                    n.close();
                }
            }
        }
        System.out.println(ok ? "RESULT: PASS（无丢失、无重复、无乱序，副本与重放一致）" : "RESULT: FAIL");
        System.exit(ok ? 0 : 1);
    }

    private static boolean verifyLive(Node[] nodes, long total) {
        boolean ok = true;
        final long ref = nodes[0].service().chain;
        for (Node n : nodes) {
            final NoopService s = n.service();
            final boolean good = s.count == total && s.violations == 0 && !s.halted && s.chain == ref;
            System.out.printf("   成员 %d：交付 %d/%d，序号违规 %d，顺序哈希 %016x %s%n", n.id(), s.count, total, s.violations, s.chain, good ? "✓" : "✗");
            ok &= good;
        }
        return ok;
    }

    private static String fmt(long nanos) {
        return nanos < 1_000_000 ? String.format("%.0fµs", nanos / 1e3) : String.format("%.1fms", nanos / 1e6);
    }
}
