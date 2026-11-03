package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import jp.trade.exchange.events.GapFillingConsumer;
import jp.trade.exchange.projection.ConnectionFactory;
import jp.trade.exchange.projection.GapException;
import jp.trade.exchange.projection.JdbcProjector;
import jp.trade.exchange.projection.KafkaProjectionRunner;
import jp.trade.exchange.projection.ProjectionAuditor;
import jp.trade.exchange.projection.ProjectionPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/** 真实 MariaDB 上的投影：幂等、事务回滚、缺口拒绝、对账、并发写入者。 */
class ProjectionTest {
    private static final GapFillingConsumer.Backfill NO_BACKFILL = new GapFillingConsumer.Backfill() {
        @Override public List<byte[]> fetch(long from, long to) { throw new AssertionError("不应需要补数"); }
        @Override public long headSeq() { throw new AssertionError("不应需要补数"); }
    };

    private static void feed(ProjectionPipeline p, List<byte[]> events) {
        for (byte[] e : events) {
            p.publish(jp.trade.exchange.core.Protocol.eventSeq(new org.agrona.concurrent.UnsafeBuffer(e), 0), e);
        }
        p.flush();
    }

    @Test
    void projectionMatchesEngineStateExactly() throws Exception {
        final ConnectionFactory f = TestDb.freshSchema();
        final EngineStream s = EngineStream.generate(7, 6_000);
        final ProjectionPipeline p = new ProjectionPipeline(new JdbcProjector(f), NO_BACKFILL, 7);
        feed(p, s.events);

        try (Connection c = f.get()) {
            final ProjectionAuditor.Result r = ProjectionAuditor.audit(c, s.engine);
            assertTrue(r.clean(), "diffs: " + r.diffs());
        }
        final long trades = s.events.stream().filter(e -> eventType(new org.agrona.concurrent.UnsafeBuffer(e), 0) == EVT_TRADE).count();
        assertTrue(trades > 100, "随机流应有足够成交: " + trades);
        assertEquals(trades, TestDb.count(f, "trades"));
        // 资产守恒（投影侧）
        try (Connection c = f.get(); var st = c.createStatement();
             var rs = st.executeQuery("SELECT asset, SUM(available + frozen) FROM account_balance GROUP BY asset")) {
            while (rs.next()) {
                assertEquals(s.engine.totalHeld(rs.getInt(1)), rs.getLong(2));
            }
        }
    }

    @Test
    void redeliveryAndOverlappingBatchesYieldIdenticalState() throws Exception {
        final EngineStream s = EngineStream.generate(11, 3_000);

        ConnectionFactory f = TestDb.freshSchema();
        new JdbcProjector(f).apply(s.events);
        final String single = TestDb.fingerprint(f);

        f = TestDb.freshSchema();
        final JdbcProjector p = new JdbcProjector(f);
        final Random rnd = new Random(5);
        int from = 0;
        while (from < s.events.size()) {
            // 每批都从“已提交点之前”的随机位置开始，制造大量重复投递，并且批长不一
            final int start = Math.max(0, from - rnd.nextInt(40));
            final int end = Math.min(s.events.size(), from + 1 + rnd.nextInt(60));
            p.apply(new ArrayList<>(s.events.subList(start, end)));
            from = end;
        }
        p.apply(new ArrayList<>(s.events)); // 整条流再投递一遍，必须完全无副作用
        assertEquals(single, TestDb.fingerprint(f));
    }

    @Test
    void crashMidBatchRollsBackEverything() throws Exception {
        final EngineStream s = EngineStream.generate(3, 500);
        final ConnectionFactory f = TestDb.freshSchema();
        final JdbcProjector p = new JdbcProjector(f);
        final String empty = TestDb.fingerprint(f);

        final java.lang.reflect.Method m = JdbcProjector.class.getDeclaredMethod("setAfterEventHook", java.util.function.LongConsumer.class);
        m.setAccessible(true);
        m.invoke(p, (java.util.function.LongConsumer) seq -> {
            if (seq == 40) {
                throw new IllegalStateException("simulated crash");
            }
        });
        assertThrows(IllegalStateException.class, () -> p.apply(s.events));
        assertEquals(empty, TestDb.fingerprint(f), "崩溃的批次不留任何痕迹，checkpoint 也没动");
        assertEquals(0, p.checkpoint());

        m.invoke(p, (java.util.function.LongConsumer) seq -> {});
        p.apply(s.events); // 重试成功
        try (Connection c = f.get()) {
            assertTrue(ProjectionAuditor.audit(c, s.engine).clean());
        }
    }

    @Test
    void gapIsRejectedAndRolledBack() throws Exception {
        final EngineStream s = EngineStream.generate(9, 200);
        final ConnectionFactory f = TestDb.freshSchema();
        final JdbcProjector p = new JdbcProjector(f);
        assertEquals(3, p.apply(new ArrayList<>(s.events.subList(0, 3))));
        final List<byte[]> withHole = new ArrayList<>(s.events.subList(3, 10));
        withHole.remove(1); // 去掉 seq 5
        final GapException g = assertThrows(GapException.class, () -> p.apply(withHole));
        assertEquals(5, g.expectedSeq);
        assertEquals(6, g.actualSeq);
        assertEquals(3, p.checkpoint(), "整批回滚，包括 seq 4");
    }

    @Test
    void concurrentWritersAreSerialisedByCheckpointLock() throws Exception {
        final EngineStream s = EngineStream.generate(21, 2_000);
        ConnectionFactory f = TestDb.freshSchema();
        new JdbcProjector(f).apply(s.events);
        final String single = TestDb.fingerprint(f);

        f = TestDb.freshSchema();
        final JdbcProjector p = new JdbcProjector(f);
        final List<Thread> threads = new ArrayList<>();
        final List<Throwable> errors = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int t = 0; t < 4; t++) {
            final Thread th = new Thread(() -> {
                try {
                    for (int i = 0; i < s.events.size(); i += 50) {
                        p.apply(new ArrayList<>(s.events.subList(i, Math.min(s.events.size(), i + 50))));
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            threads.add(th);
            th.start();
        }
        for (Thread th : threads) {
            th.join();
        }
        // 某个线程可能先到后面的批次而遇到缺口，这是允许的；但无论如何最终状态必须一致
        errors.forEach(e -> assertInstanceOf(GapException.class, e));
        p.apply(s.events);
        assertEquals(single, TestDb.fingerprint(f));
    }

    @Test
    void auditorDetectsCorruption() throws Exception {
        final EngineStream s = EngineStream.generate(13, 1_000);
        final ConnectionFactory f = TestDb.freshSchema();
        new JdbcProjector(f).apply(s.events);
        try (Connection c = f.get()) {
            assertTrue(ProjectionAuditor.audit(c, s.engine).clean());

            c.createStatement().executeUpdate("UPDATE account_balance SET available = available + 1 WHERE account_id = 1 AND asset = 2");
            assertTrue(ProjectionAuditor.audit(c, s.engine).diffs().stream().anyMatch(d -> d.startsWith("balance mismatch 1/2")));
            c.createStatement().executeUpdate("UPDATE account_balance SET available = available - 1 WHERE account_id = 1 AND asset = 2");

            c.createStatement().executeUpdate("DELETE FROM orders WHERE status = 0 LIMIT 1");
            assertTrue(ProjectionAuditor.audit(c, s.engine).diffs().stream().anyMatch(d -> d.startsWith("open order missing in DB")));
        }
    }

    @Test
    void auditorSkipsWhenProjectionLags() throws Exception {
        final EngineStream s = EngineStream.generate(17, 300);
        final ConnectionFactory f = TestDb.freshSchema();
        new JdbcProjector(f).apply(new ArrayList<>(s.events.subList(0, s.events.size() - 3)));
        try (Connection c = f.get()) {
            final ProjectionAuditor.Result r = ProjectionAuditor.audit(c, s.engine);
            assertTrue(r.skipped());
            assertFalse(r.clean());
        }
    }

    @Test
    void kafkaRunnerCommitsOffsetsAfterDbAndAbsorbsRedelivery() throws Exception {
        final EngineStream s = EngineStream.generate(5, 400);
        final ConnectionFactory f = TestDb.freshSchema();
        final JdbcProjector projector = new JdbcProjector(f);
        final ProjectionPipeline pipeline = new ProjectionPipeline(projector, NO_BACKFILL, 50);

        final TopicPartition tp = new TopicPartition("exchange.events", 0);
        final MockConsumer<byte[], byte[]> kafka = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafka.assign(List.of(tp));
        kafka.updateBeginningOffsets(Map.of(tp, 0L));
        long offset = 0;
        final byte[] key = MARKET_NAME.getBytes();
        for (byte[] e : s.events) {
            kafka.addRecord(new ConsumerRecord<>(tp.topic(), 0, offset++, key, e));
        }
        for (int i = 100; i < 160; i++) { // 重投一段
            kafka.addRecord(new ConsumerRecord<>(tp.topic(), 0, offset++, key, s.events.get(i)));
        }
        final KafkaProjectionRunner runner = new KafkaProjectionRunner(kafka, pipeline);
        assertEquals(s.events.size() + 60, runner.pollOnce(Duration.ofMillis(10)));

        assertEquals(s.events.size(), projector.checkpoint());
        assertEquals(offset, kafka.committed(java.util.Set.of(tp)).get(tp).offset());
        try (Connection c = f.get()) {
            assertTrue(ProjectionAuditor.audit(c, s.engine).clean());
        }
    }
}
