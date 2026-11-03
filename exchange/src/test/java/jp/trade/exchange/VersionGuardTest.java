package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import io.aeron.cluster.ClusterTool;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.LoggingEventSink;
import org.junit.jupiter.api.Test;

/** 状态机语义版本不兼容时，宁可不起来，也不能用新逻辑去加载旧状态。 */
class VersionGuardTest {
    @Test
    void anIncompatibleStateMachineVersionRefusesToLoadAnOlderSnapshot() throws Exception {
        final File dir = Files.createTempDirectory("ver").toFile();
        try (ExchangeNode node = ExchangeNode.start(dir, true, new LoggingEventSink(false));
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            c.deposit(1, ASSET_QUOTE, 1_000);
            c.await(1, 10_000);
            assertTrue(ClusterTool.snapshot(node.clusterDir(), System.out));
            Thread.sleep(1_500); // 等快照落盘
        }

        final int v2 = org.agrona.SemanticVersion.compose(2, 0, 0); // 新代码改变了语义：升 major
        final List<String> errors = new CopyOnWriteArrayList<>();
        try (ExchangeNode node = ExchangeNode.start(0, 1, dir, false, new LoggingEventSink(false), cfg -> {
            cfg.consensusModuleContext().appVersion(v2);
            cfg.clusteredServiceContext().appVersion(v2);
            cfg.errorHandler(t -> errors.add(String.valueOf(t)));
        })) {
            // 节点起不来：客户端连不上，也得不到任何服务
            assertThrows(Exception.class, () -> {
                try (ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
                    c.deposit(2, ASSET_QUOTE, 1_000);
                    c.await(1, 3_000);
                }
            });
            final long deadline = System.currentTimeMillis() + 5_000;
            while (errors.stream().noneMatch(e -> e.contains("incompatible")) && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue(errors.stream().anyMatch(e -> e.contains("incompatible")), "明确报告版本不兼容: " + errors);
        }
    }
}
