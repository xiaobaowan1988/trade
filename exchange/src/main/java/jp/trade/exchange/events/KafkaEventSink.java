package jp.trade.exchange.events;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import static jp.trade.exchange.core.Protocol.MARKET_NAME;

/**
 * Kafka 出口：key=市场名（同一交易对恒定落在同一 partition，保证分区内有序），value=72B 事件。
 *
 * “可以慢，不能丢”：
 *  - 生产者幂等、acks=all、无限重试、无限发送超时：Kafka 不可用时只会变慢，不会放弃；
 *  - 每条 send 都带回调，任何失败都记下来，flush() 时抛出，由发布线程对整批重试，而不是“发了就忘”。
 */
public final class KafkaEventSink implements EventSink {
    public static final String DEFAULT_TOPIC = "exchange.events";

    private static final byte[] KEY = MARKET_NAME.getBytes(StandardCharsets.UTF_8);

    private final Producer<byte[], byte[]> producer;
    private final String topic;
    private final AtomicReference<Exception> failure = new AtomicReference<>();

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
        p.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000"); // 缓冲区满时 send 最多阻塞 1 分钟，之后抛异常由发布线程重试
        return new KafkaEventSink(new KafkaProducer<>(p), topic);
    }

    @Override
    public void publish(long seq, byte[] event) {
        producer.send(new ProducerRecord<>(topic, KEY, event), (metadata, exception) -> {
            if (exception != null) {
                failure.compareAndSet(null, exception);
            }
        });
    }

    @Override
    public void flush() {
        producer.flush(); // 等到所有回调都已触发
        final Exception e = failure.getAndSet(null);
        if (e != null) {
            throw new IllegalStateException("kafka send failed: " + e, e);
        }
    }

    @Override
    public void close() {
        try {
            flush();
        } finally {
            producer.close();
        }
    }
}
