package jp.trade.exchange;

import java.io.File;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.EventSink;
import jp.trade.exchange.events.KafkaEventSink;
import jp.trade.exchange.events.LoggingEventSink;

import static jp.trade.exchange.core.Protocol.*;

/**
 * 演示：启动单节点集群 → 两个账户充值 → 互相撮合 → 打印事件。
 * 设置 -Dkafka.bootstrap=host:9092 则事件写入 Kafka，否则打印到控制台。
 */
public final class Main {
    public static void main(String[] args) throws Exception {
        final String kafka = System.getProperty("kafka.bootstrap");
        final EventSink sink = kafka != null
                ? KafkaEventSink.connect(kafka, KafkaEventSink.DEFAULT_TOPIC)
                : new LoggingEventSink(true);
        final File dir = new File(System.getProperty("exchange.dir", "target/exchange-data"));
        try (ExchangeNode node = ExchangeNode.start(dir, true, sink);
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            c.deposit(1, ASSET_QUOTE, 10_000_000); // 买方 1000 万 JPY
            c.deposit(2, ASSET_BASE, 100);         // 卖方 100 lot
            c.await(2, 5_000);
            c.place(2, SIDE_SELL, 1, 5_000, 10);   // 卖 10 @ 5000
            c.place(1, SIDE_BUY, 1, 5_100, 4);     // 买 4 @ 5100 → 以 5000 成交 4
            c.await(3, 5_000); // 卖单 ACCEPTED；买单 ACCEPTED + TRADE
            Thread.sleep(500); // 等事件桥异步刷出
        }
    }
}
