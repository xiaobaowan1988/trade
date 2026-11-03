package jp.trade.exchange.projection;

import java.util.ArrayList;
import java.util.List;
import jp.trade.exchange.events.EventSink;
import jp.trade.exchange.events.GapFillingConsumer;

/**
 * 事件源 → 补缺口/去重 → 批量事务写 MySQL。
 * GapFillingConsumer 的起点是数据库里的 last_seq（权威进度）。若一批写库失败，
 * 消费者内存里的 lastSeq 已经跑在数据库前面，所以必须 reset：丢掉缓冲并从数据库 last_seq 重新开始。
 */
public final class ProjectionPipeline implements EventSink {
    private final JdbcProjector projector;
    private final GapFillingConsumer.Backfill backfill;
    private final int batchSize;
    private final List<byte[]> buffer = new ArrayList<>();
    private GapFillingConsumer consumer;

    public ProjectionPipeline(JdbcProjector projector, GapFillingConsumer.Backfill backfill, int batchSize) {
        this.projector = projector;
        this.backfill = backfill;
        this.batchSize = batchSize;
    }

    /**
     * 丢弃缓冲，并让消费者在下次使用时从数据库 last_seq 重建。
     * 必须是“惰性”的：复位的那一刻数据库往往正是不可用的，如果在这里读 checkpoint 失败，
     * 会留下一个进度领先于数据库的消费者，静默跳过事件。惰性重建失败时保持“待重建”，下次再试。
     */
    public synchronized void reset() {
        buffer.clear();
        consumer = null;
    }

    private GapFillingConsumer consumer() {
        if (consumer == null) {
            consumer = new GapFillingConsumer(projector.checkpoint(), backfill, buffer::add); // 失败则抛出，consumer 仍为 null
        }
        return consumer;
    }

    @Override
    public synchronized void publish(long seq, byte[] event) {
        try {
            consumer().publish(seq, event);
        } catch (RuntimeException e) {
            reset();
            throw e;
        }
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    /** 把缓冲的事件在一个事务里写入数据库。失败则复位并抛出，调用方重试即可（幂等）。 */
    public synchronized void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        try {
            projector.apply(buffer);
            buffer.clear();
        } catch (RuntimeException e) {
            reset();
            throw e;
        }
    }

    /** 周期性调用：补齐尾部缺口并落库。 */
    public synchronized void catchUpToHead() {
        try {
            consumer().catchUpToHead();
        } catch (RuntimeException e) {
            reset();
            throw e;
        }
        flush();
    }

    public synchronized long lastSeqInMemory() { return consumer == null ? projector.checkpoint() : consumer.lastSeq(); }
}
