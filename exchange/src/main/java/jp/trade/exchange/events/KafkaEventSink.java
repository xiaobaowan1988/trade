package jp.trade.exchange.events;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import static jp.trade.exchange.core.Protocol.MARKET_NAME;

/**
 * Kafka 出口：key=市场名（同一交易对恒定落在同一 partition，保证分区内有序），value=72B 事件。
 * 使用幂等 Producer + acks=all；序号用于下游去重，所以这里是“至少一次 + 幂等重试”。
 */
public final class KafkaEventSink implements EventSink {
    public static final String DEFAULT_TOPIC = "exchange.events";

    private static final byte[] KEY = MARKET_NAME.getBytes(StandardCharsets.UTF_8);

    private final Producer<byte[], byte[]> producer;
    private final String topic;

    public KafkaEventSink(Producer<byte[], byte[]> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public static KafkaEventSink connect(String bootstrapServers, String topic) {
        final Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        return new KafkaEventSink(new KafkaProducer<>(p), topic);
    }

    @Override
    public void publish(long seq, byte[] event) {
        producer.send(new ProducerRecord<>(topic, KEY, event));
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
