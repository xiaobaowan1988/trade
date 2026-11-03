package jp.trade.exchange.cluster;

import io.aeron.archive.Archive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.ThreadingMode;
import io.aeron.samples.cluster.ClusterConfig;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jp.trade.exchange.events.EventBridge;
import jp.trade.exchange.events.EventSink;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;

/** 单节点（成员数=1 的 Raft 组）集群：MediaDriver + Archive + ConsensusModule + 撮合服务 + 事件桥。 */
public final class ExchangeNode implements AutoCloseable {
    public static final int PORT_BASE = 9000;
    public static final String HOST = "localhost";

    private final int memberId;
    private final File baseDir;
    private final ClusteredMediaDriver driver;
    private final ClusteredServiceContainer container;
    private final EventBridge bridge;
    private final ExchangeService service;

    private ExchangeNode(int memberId, File baseDir, ClusteredMediaDriver driver, ClusteredServiceContainer container,
                         EventBridge bridge, ExchangeService service) {
        this.memberId = memberId;
        this.baseDir = baseDir;
        this.driver = driver;
        this.container = container;
        this.bridge = bridge;
        this.service = service;
    }

    /** 单成员集群。 */
    public static ExchangeNode start(File baseDir, boolean fresh, EventSink sink) {
        return start(0, 1, baseDir, fresh, sink);
    }

    /**
     * @param memberId    本节点编号，0..memberCount-1
     * @param memberCount Raft 组成员数（1/3/5）
     * @param fresh       true 则清空数据目录从零开始；false 则从已有日志/快照恢复
     */
    public static ExchangeNode start(int memberId, int memberCount, File baseDir, boolean fresh, EventSink sink) {
        return start(memberId, memberCount, baseDir, fresh, sink, c -> {});
    }

    /** @param customizer 在启动前调整 Aeron 配置（测试里用来缩小日志/归档分段） */
    public static ExchangeNode start(int memberId, int memberCount, File baseDir, boolean fresh, EventSink sink,
                                     java.util.function.Consumer<ClusterConfig> customizer) {
        return start(memberId, memberCount, baseDir, fresh, sink, customizer, svc -> {});
    }

    /** @param serviceCustomizer 在启动前调整撮合服务（事件发布缓冲的等待策略等） */
    public static ExchangeNode start(int memberId, int memberCount, File baseDir, boolean fresh, EventSink sink,
                                     java.util.function.Consumer<ClusterConfig> customizer,
                                     java.util.function.Consumer<ExchangeService> serviceCustomizer) {
        if (fresh) {
            IoUtil.delete(baseDir, true);
        }
        baseDir.mkdirs();
        final ExchangeService service = new ExchangeService();
        serviceCustomizer.accept(service);
        final ClusterConfig config = ClusterConfig.create(memberId, hostnames(memberCount), PORT_BASE, service);
        config.baseDir(baseDir);
        config.aeronDirectoryName(new File(baseDir, "aeron").getAbsolutePath()); // 放磁盘而非 /dev/shm，容器内更稳
        config.errorHandler(Throwable::printStackTrace);
        config.mediaDriverContext().threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true);
        config.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=64k")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(200))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(2)) // 默认 10s，缩短以加快故障切换
                // Aeron 默认只允许 10 个并发会话；用户经网关复用进集群，所以这里按“网关数 + 运维连接”配置
                .maxConcurrentSessions(1_000);
        // 先持久、再确认：Aeron 默认 fileSyncLevel=0 只写页缓存，整机断电会丢“已确认”的订单。2=fsync 数据与元数据。
        config.archiveContext().fileSyncLevel(2).catalogFileSyncLevel(2);
        config.archiveContext().deleteArchiveOnStart(false).threadingMode(io.aeron.archive.ArchiveThreadingMode.SHARED);

        customizer.accept(config);
        final ClusteredMediaDriver driver = ClusteredMediaDriver.launch(
                config.mediaDriverContext(), config.archiveContext(), config.consensusModuleContext());
        final ClusteredServiceContainer container = ClusteredServiceContainer.launch(config.clusteredServiceContext());
        final EventBridge bridge = new EventBridge(config.mediaDriverContext().aeronDirectoryName(), sink);
        return new ExchangeNode(memberId, baseDir, driver, container, bridge, service);
    }

    public static List<String> hostnames(int memberCount) {
        final List<String> l = new ArrayList<>();
        for (int i = 0; i < memberCount; i++) {
            l.add(HOST);
        }
        return l;
    }

    public int memberId() { return memberId; }

    /** 本节点 Archive 控制端口（补数进程通过它读取 Raft 日志录制）。 */
    public String archiveControlEndpoint() {
        return HOST + ":" + ClusterConfig.calculatePort(memberId, PORT_BASE, ClusterConfig.ARCHIVE_CONTROL_PORT_OFFSET);
    }

    public String aeronDirectoryName() { return driver.mediaDriver().aeronDirectoryName(); }

    public File archiveDir() { return new File(baseDir, ClusterConfig.ARCHIVE_SUB_DIR); }

    public File clusterDir() { return new File(baseDir, ClusterConfig.CLUSTER_SUB_DIR); }

    public EventBridge bridge() { return bridge; }

    public ExchangeService service() { return service; }

    @Override
    public void close() {
        CloseHelper.quietClose(bridge);
        CloseHelper.quietClose(container);
        CloseHelper.quietClose(driver);
    }
}
