package jp.trade.exchange.projection;

import java.time.Duration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import static jp.trade.exchange.core.Protocol.EVENT_LENGTH;
import static jp.trade.exchange.core.Protocol.eventSeq;

/**
 * Kafka 消费循环。数据库 last_seq 才是权威进度，所以先落库、再提交 offset；
 * offset 提交失败或丢失只会导致重复消费，会被 seq 去重吸收。
 */
public final class KafkaProjectionRunner {
    private final Consumer<byte[], byte[]> kafka;
    private final ProjectionPipeline pipeline;

    public KafkaProjectionRunner(Consumer<byte[], byte[]> kafka, ProjectionPipeline pipeline) {
        this.kafka = kafka;
        this.pipeline = pipeline;
    }

    /** 拉取一批并处理，返回本次收到的记录数。 */
    public int pollOnce(Duration timeout) {
        final ConsumerRecords<byte[], byte[]> records = kafka.poll(timeout);
        for (ConsumerRecord<byte[], byte[]> r : records) {
            final byte[] v = r.value();
            if (v == null || v.length != EVENT_LENGTH) {
                continue; // 骨架：忽略畸形记录；生产应进死信队列
            }
            pipeline.publish(eventSeq(new org.agrona.concurrent.UnsafeBuffer(v), 0), v);
        }
        pipeline.flush();
        if (!records.isEmpty()) {
            kafka.commitSync();
        }
        return records.count();
    }
}
