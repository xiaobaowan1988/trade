package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongPredicate;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.EventSink;
import jp.trade.exchange.events.GapFillingConsumer;
import jp.trade.exchange.events.LogReplayer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/** 事件丢失后，由 Raft 日志确定性重放补数，下游最终得到连续且与 leader 当时产生的逐字节一致的事件流。 */
class BackfillTest {

    /** 记录 leader 实际产生的全部事件（真值），并按规则把一部分“丢掉”不交给下游，模拟 Kafka/发布丢失。 */
    static final class LossySink implements EventSink {
        final List<byte[]> truth = Collections.synchronizedList(new ArrayList<>());
        final GapFillingConsumer downstream;
        final LongPredicate drop;

        LossySink(GapFillingConsumer downstream, LongPredicate drop) {
            this.downstream = downstream;
            this.drop = drop;
        }

        @Override
        public void publish(long seq, byte[] event) {
            truth.add(event);
            if (!drop.test(seq)) {
                downstream.publish(seq, event);
            }
        }
    }

    private static long seq(byte[] e) { return eventSeq(new UnsafeBuffer(e), 0); }

    @Test
    void fillsMiddleAndTailGapsByDeterministicLogReplay() throws Exception {
        final File dir = Files.createTempDirectory("exch-bf").toFile();
        final List<byte[]> delivered = new ArrayList<>();

        final ExchangeNode[] holder = new ExchangeNode[1];
        // 补数源在节点启动后才能创建，所以用懒加载代理
        final LogReplayer[] replayer = new LogReplayer[1];
        final GapFillingConsumer.Backfill lazy = new GapFillingConsumer.Backfill() {
            @Override public List<byte[]> fetch(long from, long to) { return replayer[0].fetch(from, to); }
            @Override public long headSeq() { return replayer[0].headSeq(); }
        };
        final GapFillingConsumer consumer = new GapFillingConsumer(0, lazy, delivered::add);
        // 丢弃：序号 1（头部）、5..9（中间）、以及最后 3 条（尾部，只能靠 catchUpToHead 发现）
        final long total = 40;
        final LossySink sink = new LossySink(consumer, s -> s == 1 || (s >= 5 && s <= 9) || s > total - 3);

        try (ExchangeNode node = ExchangeNode.start(dir, true, sink)) {
            holder[0] = node;
            replayer[0] = new LogReplayer(node.clusterDir(), node.aeronDirectoryName(), node.archiveControlEndpoint());
            try (ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
                c.deposit(1, ASSET_QUOTE, 10_000_000);   // seq 1
                c.deposit(2, ASSET_BASE, 1_000);         // seq 2
                int expected = 2;
                for (int i = 1; i <= 12; i++) {
                    c.place(2, SIDE_SELL, i, 5_000 + i, 3);   // ACCEPTED
                    expected++;
                }
                for (int i = 1; i <= 6; i++) {
                    c.place(1, SIDE_BUY, i, 5_020, 4);        // ACCEPTED + 若干 TRADE
                    expected++;
                }
                c.await(expected, 15_000);
                // 补齐到恰好 40 条：用不会成交的买单
                long seqNow = 0;
                while ((seqNow = sink.truth.size()) < total) {
                    c.place(1, SIDE_BUY, 1_000 + (int) seqNow, 100, 1);
                    c.await(1, 5_000);
                    Thread.sleep(20);
                }
                final long deadline = System.currentTimeMillis() + 10_000;
                while (sink.truth.size() < total && System.currentTimeMillis() < deadline) {
                    Thread.sleep(20);
                }
            }
            assertTrue(sink.truth.size() >= total);
            assertTrue(consumer.lastSeq() < sink.truth.size(), "尾部确实丢了，补数前下游落后");

            consumer.catchUpToHead(); // 发现并补齐尾部缺口
            replayer[0].close();
        }

        // 下游：序号严格连续 1..N，且每条与 leader 当时产生的事件逐字节相同
        assertEquals(sink.truth.size(), delivered.size());
        for (int i = 0; i < delivered.size(); i++) {
            assertEquals(i + 1, seq(delivered.get(i)));
            assertArrayEquals(sink.truth.get(i), delivered.get(i), "event " + (i + 1) + " 字节一致");
        }
        assertTrue(consumer.filled() >= 1 + 5 + 3, "头部、中间、尾部缺口都由补数填上");
        System.out.println("[backfill] events=" + delivered.size() + " filled=" + consumer.filled());
    }
}
