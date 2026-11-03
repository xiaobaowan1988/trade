package jp.trade.exchange.cluster;

import io.aeron.archive.Archive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.ThreadingMode;
import io.aeron.samples.cluster.ClusterConfig;
import java.io.File;
import java.util.List;
import jp.trade.exchange.events.EventBridge;
import jp.trade.exchange.events.EventSink;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;

/** 单节点（成员数=1 的 Raft 组）集群：MediaDriver + Archive + ConsensusModule + 撮合服务 + 事件桥。 */
public final class ExchangeNode implements AutoCloseable {
    public static final int PORT_BASE = 9000;
    public static final String HOST = "localhost";

    private final File baseDir;
    private final ClusteredMediaDriver driver;
    private final ClusteredServiceContainer container;
    private final EventBridge bridge;
    private final ExchangeService service;

    private ExchangeNode(File baseDir, ClusteredMediaDriver driver, ClusteredServiceContainer container,
                         EventBridge bridge, ExchangeService service) {
        this.baseDir = baseDir;
        this.driver = driver;
        this.container = container;
        this.bridge = bridge;
        this.service = service;
    }

    /** @param fresh true 则清空数据目录从零开始；false 则从已有日志/快照恢复 */
    public static ExchangeNode start(File baseDir, boolean fresh, EventSink sink) {
        if (fresh) {
            IoUtil.delete(baseDir, true);
        }
        baseDir.mkdirs();
        final ExchangeService service = new ExchangeService();
        final ClusterConfig config = ClusterConfig.create(0, List.of(HOST), PORT_BASE, service);
        config.baseDir(baseDir);
        config.aeronDirectoryName(new File(baseDir, "aeron").getAbsolutePath()); // 放磁盘而非 /dev/shm，容器内更稳
        config.errorHandler(Throwable::printStackTrace);
        config.mediaDriverContext().threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true);
        config.consensusModuleContext().ingressChannel("aeron:udp?term-length=64k");
        config.archiveContext().deleteArchiveOnStart(false).threadingMode(io.aeron.archive.ArchiveThreadingMode.SHARED);

        final ClusteredMediaDriver driver = ClusteredMediaDriver.launch(
                config.mediaDriverContext(), config.archiveContext(), config.consensusModuleContext());
        final ClusteredServiceContainer container = ClusteredServiceContainer.launch(config.clusteredServiceContext());
        final EventBridge bridge = new EventBridge(config.mediaDriverContext().aeronDirectoryName(), sink);
        return new ExchangeNode(baseDir, driver, container, bridge, service);
    }

    public String aeronDirectoryName() { return driver.mediaDriver().aeronDirectoryName(); }

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
