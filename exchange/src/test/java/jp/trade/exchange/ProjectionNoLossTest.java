package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import jp.trade.exchange.events.GapFillingConsumer;
import jp.trade.exchange.projection.ConnectionFactory;
import jp.trade.exchange.projection.JdbcProjector;
import jp.trade.exchange.projection.KafkaProjectionRunner;
import jp.trade.exchange.projection.ProjectionAuditor;
import jp.trade.exchange.projection.ProjectionPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/** “可以慢，不能丢”在消费端与投影上的体现：写库失败、消费循环异常、尾部事件缺失，最终都必须补齐。 */
class ProjectionNoLossTest {
    /** 内存里的“Raft 日志”：补数源。 */
    private static GapFillingConsumer.Backfill logOf(List<byte[]> events) {
        return new GapFillingConsumer.Backfill() {
            @Override
            public List<byte[]> fetch(long from, long to) {
                return new ArrayList<>(events.subList((int) from - 1, (int) to));
            }

            @Override
            public long headSeq() { return events.size(); }
        };
    }

    private static long seqOf(byte[] e) { return eventSeq(new org.agrona.concurrent.UnsafeBuffer(e), 0); }

    /** 第 n 次（从 1 数）获取连接时抛 SQLException，模拟数据库闪断。 */
    private static ConnectionFactory flaky(ConnectionFactory real, AtomicInteger calls, int... failOn) {
        return () -> {
            final int n = calls.incrementAndGet();
            for (int f : failOn) {
                if (n == f) {
                    throw new SQLException("simulated database outage #" + n);
                }
            }
            return real.get();
        };
    }

    @Test
    void databaseFailuresMidStreamNeverLoseEvents() throws Exception {
        final EngineStream s = EngineStream.generate(31, 3_000);
        final ConnectionFactory real = TestDb.freshSchema();
        final AtomicInteger calls = new AtomicInteger();
        // 管线自己也会为 checkpoint 取连接，所以按“第几次取连接”失败，覆盖批中间与最后一批
        final JdbcProjector projector = new JdbcProjector(flaky(real, calls, 6, 14, 30, 31));
        final ProjectionPipeline pipeline = new ProjectionPipeline(projector, logOf(s.events), 40);

        int exceptions = 0;
        for (byte[] e : s.events) {
            try {
                pipeline.publish(seqOf(e), e);
            } catch (RuntimeException ex) {
                exceptions++; // 调用方（消费循环）只记录并继续喂后面的事件
            }
        }
        try {
            pipeline.flush();
        } catch (RuntimeException ex) {
            exceptions++;
        }
        assertTrue(exceptions >= 1, "数据库故障确实发生了");

        // 失败期间 Kafka 位置已经前移、那些事件被“跳过”了；定时兜底从日志补齐
        pipeline.catchUpToHead();
        assertEquals(s.events.size(), new JdbcProjector(real).checkpoint());
        try (Connection c = real.get()) {
            final ProjectionAuditor.Result r = ProjectionAuditor.audit(c, s.engine);
            assertTrue(r.clean(), "diffs: " + r.diffs());
        }
    }

    @Test
    void idleCatchUpFindsEventsLostAtTheTailWithoutAnyLaterEvent() throws Exception {
        final EngineStream s = EngineStream.generate(33, 1_000);
        final ConnectionFactory db = TestDb.freshSchema();
        final JdbcProjector projector = new JdbcProjector(db);
        final ProjectionPipeline pipeline = new ProjectionPipeline(projector, logOf(s.events), 100);

        final TopicPartition tp = new TopicPartition("exchange.events", 0);
        final MockConsumer<byte[], byte[]> kafka = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafka.assign(List.of(tp));
        kafka.updateBeginningOffsets(Map.of(tp, 0L));
        final int delivered = s.events.size() - 25; // 最后 25 条事件丢在了 Kafka 之前，之后再也没有新事件
        long offset = 0;
        for (int i = 0; i < delivered; i++) {
            kafka.addRecord(new ConsumerRecord<>(tp.topic(), 0, offset++, new byte[0], s.events.get(i)));
        }
        final KafkaProjectionRunner runner = new KafkaProjectionRunner(kafka, pipeline, 0);

        assertEquals(delivered, runner.pollOnce(Duration.ofMillis(10)));
        assertEquals(delivered, projector.checkpoint(), "只收到的这部分");
        runner.pollOnce(Duration.ofMillis(10)); // 空闲轮询 → 定时兜底发现并补齐尾部
        assertEquals(s.events.size(), projector.checkpoint(), "尾部缺口被补齐，没有靠任何后续事件触发");
        try (Connection c = db.get()) {
            assertTrue(ProjectionAuditor.audit(c, s.engine).clean());
        }
    }

    @Test
    void runLoopSurvivesConsumerAndDatabaseErrors() throws Exception {
        final EngineStream s = EngineStream.generate(35, 800);
        final ConnectionFactory real = TestDb.freshSchema();
        final AtomicInteger calls = new AtomicInteger();
        final JdbcProjector projector = new JdbcProjector(flaky(real, calls, 4, 5));
        final ProjectionPipeline pipeline = new ProjectionPipeline(projector, logOf(s.events), 50);

        final TopicPartition tp = new TopicPartition("exchange.events", 0);
        final MockConsumer<byte[], byte[]> kafka = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafka.assign(List.of(tp));
        kafka.updateBeginningOffsets(Map.of(tp, 0L));
        long offset = 0;
        for (byte[] e : s.events) {
            kafka.addRecord(new ConsumerRecord<>(tp.topic(), 0, offset++, new byte[0], e));
        }
        kafka.setPollException(new KafkaException("simulated broker error")); // 第一次 poll 就抛

        final KafkaProjectionRunner runner = new KafkaProjectionRunner(kafka, pipeline, 50);
        final java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);
        final Thread t = new Thread(() -> runner.run(running::get, Duration.ofMillis(10)), "runner");
        t.start();
        final JdbcProjector observer = new JdbcProjector(real); // 观察进度用不会故障的连接，避免消耗注入的故障次数
        try {
            final long deadline = System.currentTimeMillis() + 60_000;
            while (observer.checkpoint() < s.events.size() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
        } finally {
            running.set(false); // 无论如何都要停掉线程，不能泄漏到别的测试里
            t.join(5_000);
        }

        assertEquals(s.events.size(), observer.checkpoint(), "循环没有死，最终全部投影");
        assertTrue(runner.failures() >= 1, "错误被记录而不是吞掉: " + runner.failures());
        assertNotNull(runner.lastError());
        try (Connection c = real.get()) {
            assertTrue(ProjectionAuditor.audit(c, s.engine).clean());
        }
    }

    /**
     * 数据库宕机一直持续到事件流结束，最后一次写库恰好发生在宕机期间，且复位时读 checkpoint 也失败；
     * 恢复之后再也没有新事件。旧写法留下一个“以为自己已追到日志头”的陈旧消费者：缓冲为空、没有后续事件
     * 触发序号校验，定时补数也看不到缺口，数据库就永远落后。惰性复位保证复位失败时保持“待重建”。
     */
    @Test
    void outageThatEndsAfterTheLastEventStillCatchesUpFromTheLog() throws Exception {
        final EngineStream s = EngineStream.generate(37, 2_000);
        final List<byte[]> events = new ArrayList<>(s.events.subList(0, 1_000)); // 恰好是批大小的整数倍：最后一个事件触发最后一次写库

        ConnectionFactory real = TestDb.freshSchema();
        new JdbcProjector(real).apply(events);
        final String reference = TestDb.fingerprint(real); // 一次性写入的参照终态

        real = TestDb.freshSchema();
        final ConnectionFactory db = real;
        final java.util.concurrent.atomic.AtomicBoolean down = new java.util.concurrent.atomic.AtomicBoolean();
        final ConnectionFactory switchable = () -> {
            if (down.get()) {
                throw new SQLException("database is down");
            }
            return db.get();
        };
        final ProjectionPipeline pipeline = new ProjectionPipeline(new JdbcProjector(switchable), logOf(events), 50);

        for (int i = 0; i < events.size(); i++) {
            if (i == 300) {
                down.set(true); // 宕机，并且一直持续到事件流结束
            }
            try {
                pipeline.publish(seqOf(events.get(i)), events.get(i));
            } catch (RuntimeException ignored) {
                // 消费循环只记录并继续
            }
        }
        down.set(false); // 事件流已经结束，数据库此时才恢复；再也不会有新事件
        pipeline.flush();
        pipeline.catchUpToHead(); // 唯一的机会：定时补数

        assertEquals(events.size(), new JdbcProjector(db).checkpoint(), "全部补齐");
        assertEquals(reference, TestDb.fingerprint(db), "与一次性写入的终态完全一致");
    }
}
