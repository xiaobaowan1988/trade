package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.util.List;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.core.MatchingEngine;
import jp.trade.exchange.events.EventSink;
import jp.trade.exchange.events.GapFillingConsumer;
import jp.trade.exchange.events.LogReplayer;
import jp.trade.exchange.projection.ConnectionFactory;
import jp.trade.exchange.projection.JdbcProjector;
import jp.trade.exchange.projection.ProjectionAuditor;
import jp.trade.exchange.projection.ProjectionPipeline;
import org.junit.jupiter.api.Test;

/**
 * 全链路：真实 Aeron 集群 → 事件出口（人为丢事件）→ 补缺口/去重 → 事务写真实 MariaDB
 * → 与“日志确定性重放得到的撮合状态”逐账户逐订单对账。
 */
class ClusterProjectionTest {
    @Test
    void lossyEventStreamStillProjectsExactly() throws Exception {
        final ConnectionFactory db = TestDb.freshSchema();
        final File dir = Files.createTempDirectory("exch-proj").toFile();
        final LogReplayer[] replayer = new LogReplayer[1];
        final GapFillingConsumer.Backfill lazy = new GapFillingConsumer.Backfill() {
            @Override public List<byte[]> fetch(long from, long to) { return replayer[0].fetch(from, to); }
            @Override public long headSeq() { return replayer[0].headSeq(); }
        };
        final ProjectionPipeline pipeline = new ProjectionPipeline(new JdbcProjector(db), lazy, 16);
        final long total = 60;
        // 丢弃：序号 4..6、30、以及最后若干条
        final EventSink lossy = (seq, e) -> {
            if (seq >= 4 && seq <= 6 || seq == 30 || seq > total - 5) {
                return;
            }
            pipeline.publish(seq, e);
        };

        try (ExchangeNode node = ExchangeNode.start(dir, true, lossy)) {
            replayer[0] = new LogReplayer(node.clusterDir(), node.aeronDirectoryName(), node.archiveControlEndpoint());
            try (ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
                for (long a = 1; a <= 4; a++) {
                    c.deposit(a, ASSET_QUOTE, 5_000_000);
                    c.deposit(a, ASSET_BASE, 1_000);
                }
                c.await(8, 15_000);
                int n = 8;
                int cid = 1;
                while (n < 100) { // 买卖交叉，价格有改善，部分成交、挂单、撤单混合
                    final long a = 1 + (cid % 4);
                    c.place(a, cid % 2 == 0 ? SIDE_BUY : SIDE_SELL, cid, 5_000 + (cid % 7) * 10, 1 + (cid % 5));
                    n++;
                    cid++;
                    if (cid % 9 == 0) {
                        c.cancel(a, 1 + (cid % 20));
                        n++;
                    }
                    if (n % 10 == 0) {
                        Thread.sleep(5);
                    }
                }
                Thread.sleep(500);
            }
            pipeline.catchUpToHead(); // 发现并补齐缺口（含尾部），再落库

            final MatchingEngine truth = replayer[0].rebuildState();
            assertTrue(truth.lastEventSeq() > total, "事件数 " + truth.lastEventSeq());
            try (Connection conn = db.get()) {
                final ProjectionAuditor.Result r = ProjectionAuditor.audit(conn, truth);
                assertTrue(r.clean(), "skipped=" + r.skipped() + " diffs=" + r.diffs());
            }
            assertTrue(TestDb.count(db, "trades") > 0);
            System.out.println("[projection] head=" + truth.lastEventSeq() + " trades=" + TestDb.count(db, "trades"));
            replayer[0].close();
        }
    }
}
