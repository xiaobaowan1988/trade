package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import jp.trade.exchange.events.GapFillingConsumer;
import jp.trade.exchange.projection.ConnectionFactory;
import jp.trade.exchange.projection.JdbcProjector;
import jp.trade.exchange.projection.KafkaProjectionRunner;
import jp.trade.exchange.projection.ProjectionIntegrityException;
import jp.trade.exchange.projection.ProjectionPipeline;
import org.agrona.ExpandableArrayBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/** “服务可以挂，但不能出错”在投影上的体现：遇到不自洽的数据就停，不重试、不写入。 */
class ProjectionIntegrityTest {
    private static byte[] event(int type, int aux, long seq, long orderId, long account, long price, long qty, long cOrder, long cAcct) {
        final ExpandableArrayBuffer b = new ExpandableArrayBuffer(EVENT_LENGTH);
        encodeEvent(b, 0, type, aux, seq, 1_000 + seq, orderId, account, price, qty, cOrder, cAcct);
        final byte[] e = new byte[EVENT_LENGTH];
        b.getBytes(0, e);
        return e;
    }

    /** 入金 100，却下了需要冻结 500 的买单：不可能由正确的撮合引擎产生，投影必须拒绝。 */
    private static List<byte[]> inconsistentStream() {
        return List.of(
                event(EVT_DEPOSITED, 0, 1, 0, 1, ASSET_QUOTE, 100, 11, 0),
                event(EVT_ACCEPTED, SIDE_BUY, 2, 1, 1, 100, 5, 1, 0));
    }

    private static final GapFillingConsumer.Backfill NONE = new GapFillingConsumer.Backfill() {
        @Override public List<byte[]> fetch(long from, long to) { throw new AssertionError("不应补数"); }
        @Override public long headSeq() { return 0; }
    };

    @Test
    void anInconsistentBatchIsRejectedByTheDatabaseAndLeavesNoTrace() throws Exception {
        final ConnectionFactory db = TestDb.freshSchema();
        final String empty = TestDb.fingerprint(db);
        final JdbcProjector projector = new JdbcProjector(db);

        final ProjectionIntegrityException ex = assertThrows(ProjectionIntegrityException.class, () -> projector.apply(inconsistentStream()));
        assertTrue(ex.getMessage().toLowerCase().contains("constraint") || ex.getCause() != null, ex.getMessage());
        assertEquals(empty, TestDb.fingerprint(db), "整批回滚：余额、订单、checkpoint 都没动");
        assertEquals(0, projector.checkpoint());
    }

    @Test
    void anUnknownEventTypeIsAnIntegrityErrorNotSomethingToRetry() throws Exception {
        final ConnectionFactory db = TestDb.freshSchema();
        final String empty = TestDb.fingerprint(db);
        final byte[] unknown = event(99, 0, 1, 0, 1, 0, 0, 0, 0); // 新版本状态机才会产生的事件类型
        assertThrows(ProjectionIntegrityException.class, () -> new JdbcProjector(db).apply(List.of(unknown)));
        assertEquals(empty, TestDb.fingerprint(db));
    }

    @Test
    void theRunLoopStopsOnIntegrityErrorsInsteadOfRetryingForever() throws Exception {
        final ConnectionFactory db = TestDb.freshSchema();
        final String empty = TestDb.fingerprint(db);
        final JdbcProjector projector = new JdbcProjector(db);
        final ProjectionPipeline pipeline = new ProjectionPipeline(projector, NONE, 10);

        final TopicPartition tp = new TopicPartition("exchange.events", 0);
        final MockConsumer<byte[], byte[]> kafka = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafka.assign(List.of(tp));
        kafka.updateBeginningOffsets(Map.of(tp, 0L));
        long offset = 0;
        for (byte[] e : inconsistentStream()) {
            kafka.addRecord(new ConsumerRecord<>(tp.topic(), 0, offset++, new byte[0], e));
        }

        final KafkaProjectionRunner runner = new KafkaProjectionRunner(kafka, pipeline, 50);
        final Thread t = new Thread(() -> runner.run(() -> true, Duration.ofMillis(10)), "runner");
        t.start();
        t.join(10_000);

        assertFalse(t.isAlive(), "遇到不自洽数据，运行循环自己停止（而不是退避重试到永远）");
        assertInstanceOf(ProjectionIntegrityException.class, runner.fatalError());
        assertEquals(1, runner.failures(), "只失败一次就停，没有重试");
        assertEquals(empty, TestDb.fingerprint(db), "没有写入任何数据");
        assertNull(kafka.committed(java.util.Set.of(tp)).get(tp), "没有提交 offset：事件没有被确认处理");
    }

    @Test
    void cleanBatchesStillProjectNormally() throws Exception {
        final EngineStream s = EngineStream.generate(41, 500);
        final ConnectionFactory db = TestDb.freshSchema();
        new JdbcProjector(db).apply(new ArrayList<>(s.events));
        assertEquals(s.events.size(), new JdbcProjector(db).checkpoint());
    }

    /**
     * 增量是负的（冻结 = available 减少）但合并后的余额合法：必须被接受。
     * 回归测试：曾经用 INSERT ... ON DUPLICATE KEY UPDATE 插入增量，被数据库对“插入元组”的 CHECK 拒绝。
     */
    @Test
    void negativeDeltasAcrossBatchBoundariesAreAcceptedWhenTheResultingBalanceIsValid() throws Exception {
        final ConnectionFactory db = TestDb.freshSchema();
        final JdbcProjector p = new JdbcProjector(db);
        p.apply(List.of(event(EVT_DEPOSITED, 0, 1, 0, 1, ASSET_QUOTE, 1_000, 11, 0)));                  // 批 1：入金
        p.apply(List.of(event(EVT_ACCEPTED, SIDE_BUY, 2, 1, 1, 100, 5, 1, 0)));                          // 批 2：冻结 500（增量为负）
        try (java.sql.Connection c = db.get(); java.sql.Statement st = c.createStatement();
             java.sql.ResultSet rs = st.executeQuery("SELECT available, frozen FROM account_balance WHERE account_id = 1 AND asset = 2")) {
            assertTrue(rs.next());
            assertEquals(500, rs.getLong(1));
            assertEquals(500, rs.getLong(2));
        }
    }
}
