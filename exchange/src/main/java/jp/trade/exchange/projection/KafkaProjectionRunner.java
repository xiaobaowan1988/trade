package jp.trade.exchange.projection;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import static jp.trade.exchange.core.Protocol.EVENT_LENGTH;
import static jp.trade.exchange.core.Protocol.eventSeq;

/**
 * Kafka 消费循环。数据库 last_seq 才是权威进度，所以先落库、再提交 offset；
 * offset 提交失败或丢失只会导致重复消费，会被 seq 去重吸收。
 *
 * “可以慢，不能丢”：
 *  - 空闲时按 catchUpIntervalMs 周期性向补数源询问日志头并补齐，否则“最后几条事件丢了、之后没有新事件”
 *    永远发现不了（只靠收到事件时检查序号是不够的）；
 *  - 写库失败后管线会从数据库 last_seq 重新开始，后续事件的序号缺口会触发补数，不会因为 Kafka 位置已经前移而丢；
 *  - run() 循环不会因异常退出：记录、退避、继续。
 */
public final class KafkaProjectionRunner {
    private final Consumer<byte[], byte[]> kafka;
    private final ProjectionPipeline pipeline;
    private final long catchUpIntervalMs;
    private long lastCatchUp = System.currentTimeMillis();
    private final AtomicLong failures = new AtomicLong();
    private volatile Throwable lastError;
    private volatile Throwable fatalError;

    public KafkaProjectionRunner(Consumer<byte[], byte[]> kafka, ProjectionPipeline pipeline) {
        this(kafka, pipeline, 5_000);
    }

    public KafkaProjectionRunner(Consumer<byte[], byte[]> kafka, ProjectionPipeline pipeline, long catchUpIntervalMs) {
        this.kafka = kafka;
        this.pipeline = pipeline;
        this.catchUpIntervalMs = catchUpIntervalMs;
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
        } else if (System.currentTimeMillis() - lastCatchUp >= catchUpIntervalMs) {
            pipeline.catchUpToHead(); // 空闲时的定时兜底：发现并补齐尾部缺口
            lastCatchUp = System.currentTimeMillis();
        }
        return records.count();
    }

    /** 持续运行直到 running 返回 false。任何异常都只记录并退避重试，循环不会死掉。 */
    public void run(BooleanSupplier running, Duration pollTimeout) {
        long backoffMs = 50;
        while (running.getAsBoolean()) {
            try {
                pollOnce(pollTimeout);
                backoffMs = 50;
            } catch (ProjectionIntegrityException e) {
                // 不可重试：数据不自洽。停下并留下原因，等人处理；绝不继续写入
                failures.incrementAndGet();
                lastError = e;
                fatalError = e;
                return;
            } catch (RuntimeException e) {
                failures.incrementAndGet();
                lastError = e;
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                backoffMs = Math.min(backoffMs * 2, 5_000);
            }
        }
    }

    public long failures() { return failures.get(); }

    public Throwable lastError() { return lastError; }

    /** 非 null 表示运行循环因数据不自洽而停止，需要人工介入。 */
    public Throwable fatalError() { return fatalError; }
}
