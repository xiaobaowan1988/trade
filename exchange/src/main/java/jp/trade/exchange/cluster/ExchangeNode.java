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
    /**
     * 生产默认的协议超时（秒）。测试在同一个 JVM 里连续起停很多节点，用 Aeron 的默认值（5/10/10 秒）让遗留连接尽快被清掉，
     * 见 pom.xml 的 argLine；高并发场景由 `./load.sh correctness` 在这些生产默认值下验证。
     */
    public static final long DEFAULT_PUBLICATION_CONNECTION_SEC = 60;
    public static final long DEFAULT_IMAGE_LIVENESS_SEC = 60;
    public static final long DEFAULT_SESSION_TIMEOUT_SEC = 60;
    /**
     * 状态机语义版本（major.minor.patch）。**任何改变命令处理结果的改动都必须升版本**（至少 major）：
     * Aeron 会拒绝用不兼容版本加载旧快照/日志，而不是用新逻辑重放旧日志得到另一个状态。
     */
    public static final int STATE_MACHINE_VERSION = org.agrona.SemanticVersion.compose(1, 0, 0);
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
        // 默认专用线程：共享模式下发送/接收/管理都挤在一个线程里，几百个会话就会让它满载，保活被拖住、
        // 会话被判超时关闭，之后这些会话上的命令被静默丢弃（实测，见设计 §5.5）。核数很少的开发/测试环境可以
        // 用 -Dexchange.threading=shared 切回共享。
        final boolean dedicated = !"shared".equals(System.getProperty("exchange.threading", "dedicated"));
        // 发布连接超时（默认 5 秒）：发布端 5 秒收不到接收端的状态消息就认为连接已断、停止发送（连心跳也不发），
        // 接收端的镜像随之不可用，共识模块立刻把该会话以 TIMEOUT 关闭，之后该会话上的命令被静默丢弃。
        // 几百个会话 + CPU 吃满时，状态消息会被拖到超过 5 秒（实测：600 会话下数秒内 54 到 590 个会话被关闭；
        // 只把这一个超时放宽到 60 秒，会话全部存活、所有正确性检查通过；只放宽镜像存活超时则没有用）。
        // 按“可以慢，不能出错”的原则放宽；Raft 的故障检测由 leader 心跳超时负责，不受影响。
        // 注意：这个值在**发送端的驱动**上生效，网关自己的 MediaDriver 也必须同样设置。
        config.mediaDriverContext().publicationConnectionTimeoutNs(
                java.util.concurrent.TimeUnit.SECONDS.toNanos(Long.getLong("exchange.publicationConnectionSec", DEFAULT_PUBLICATION_CONNECTION_SEC)));
        // 同理：会话超时与镜像存活超时也必须比最坏的“慢”更宽。实测 30 万条同时在途时排队延迟达到 10 秒，
        // 超过默认的 10 秒会话超时，入口被背压的会话连保活都发不出去，被集群以 TIMEOUT 关闭。
        // 这三个超时要配套：发布连接 ≤ 镜像存活，会话超时不小于它们；客户端的连接超时要大于发布连接超时。
        config.mediaDriverContext().imageLivenessTimeoutNs(
                java.util.concurrent.TimeUnit.SECONDS.toNanos(Long.getLong("exchange.imageLivenessSec", DEFAULT_IMAGE_LIVENESS_SEC)));
        config.mediaDriverContext().threadingMode(dedicated ? ThreadingMode.DEDICATED : ThreadingMode.SHARED).dirDeleteOnStart(true);
        config.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=64k")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(200))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(2)) // 默认 10s，缩短以加快故障切换
                // Aeron 默认只允许 10 个并发会话；用户经网关复用进集群，所以这里按“网关数 + 运维连接”配置
                // 并发会话上限：已验证 600 个会话 + 满载下无错（见设计 §5.5）；1000 个会话时连建立都不可靠。
                // 超出上限的会话被明确拒绝，而不是让所有会话一起降级。用户经网关复用，正常只需要几个到几十个会话。
                .sessionTimeoutNs(java.util.concurrent.TimeUnit.SECONDS.toNanos(Long.getLong("exchange.sessionTimeoutSec", DEFAULT_SESSION_TIMEOUT_SEC)))
                .maxConcurrentSessions(Integer.getInteger("exchange.maxSessions", 512));
        config.consensusModuleContext().appVersion(STATE_MACHINE_VERSION);
        config.clusteredServiceContext().appVersion(STATE_MACHINE_VERSION);
        // 先持久、再确认：Aeron 默认 fileSyncLevel=0 只写页缓存，整机断电会丢“已确认”的订单。2=fsync 数据与元数据。
        config.archiveContext().fileSyncLevel(2).catalogFileSyncLevel(2);
        // 存储完整性：录制时为每个帧计算 CRC32C，重放时校验。静默位翻转会被发现，而不是变成“另一条合法的命令”
        config.archiveContext()
                .recordChecksum(io.aeron.archive.checksum.Checksums.crc32c())
                .replayChecksum(io.aeron.archive.checksum.Checksums.crc32c());
        config.archiveContext().deleteArchiveOnStart(false)
                .threadingMode(dedicated ? io.aeron.archive.ArchiveThreadingMode.DEDICATED : io.aeron.archive.ArchiveThreadingMode.SHARED);

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

    /** 驱动的发布连接超时（纳秒）。 */
    public long publicationConnectionTimeoutNs() { return driver.mediaDriver().context().publicationConnectionTimeoutNs(); }

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
