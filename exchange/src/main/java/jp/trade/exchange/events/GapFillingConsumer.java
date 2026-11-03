package jp.trade.exchange.events;

import java.util.List;
import java.util.function.Consumer;

import static jp.trade.exchange.core.Protocol.EVENT_LENGTH;
import static jp.trade.exchange.core.Protocol.eventSeq;

/**
 * 下游消费者的参考实现（Flink 作业里同样的逻辑）：保存已处理的 lastSeq（checkpoint 状态），
 * 丢弃重复，遇到缺口先向补数源取回缺失区间，再继续，因此交给下游的事件序号严格连续、不重不漏。
 */
public final class GapFillingConsumer implements EventSink {
    /** 补数源：按序号区间（含）返回事件，必须严格连续。 */
    public interface Backfill {
        List<byte[]> fetch(long fromSeq, long toSeq);

        /** 当前已提交日志产生的最大事件序号，用于发现“尾部缺口”。 */
        long headSeq();
    }

    private final Backfill backfill;
    private final Consumer<byte[]> downstream;
    private long lastSeq;
    private long duplicates;
    private long filled;

    /** @param checkpointedLastSeq 上次 checkpoint 时已处理的最大序号，全新作业为 0 */
    public GapFillingConsumer(long checkpointedLastSeq, Backfill backfill, Consumer<byte[]> downstream) {
        this.lastSeq = checkpointedLastSeq;
        this.backfill = backfill;
        this.downstream = downstream;
    }

    @Override
    public synchronized void publish(long seq, byte[] event) {
        if (seq <= lastSeq) {
            duplicates++;
            return;
        }
        if (seq > lastSeq + 1) {
            fill(lastSeq + 1, seq - 1);
        }
        deliver(event);
    }

    /** 没有新事件到来时的周期性检查：补齐到日志头，覆盖“最后几条事件丢失”的情况。 */
    public synchronized void catchUpToHead() {
        final long head = backfill.headSeq();
        if (head > lastSeq) {
            fill(lastSeq + 1, head);
        }
    }

    private void fill(long from, long to) {
        final List<byte[]> missing = backfill.fetch(from, to);
        for (byte[] e : missing) {
            deliver(e);
        }
        if (lastSeq != to) {
            throw new IllegalStateException("补数未能补齐 [" + from + "," + to + "]，当前 lastSeq=" + lastSeq);
        }
        filled += to - from + 1;
    }

    private void deliver(byte[] e) {
        if (e.length != EVENT_LENGTH) {
            throw new IllegalStateException("bad event length " + e.length);
        }
        final long seq = eventSeq(new org.agrona.concurrent.UnsafeBuffer(e), 0);
        if (seq != lastSeq + 1) {
            throw new IllegalStateException("非连续事件 seq=" + seq + " last=" + lastSeq);
        }
        lastSeq = seq;
        downstream.accept(e);
    }

    public synchronized long lastSeq() { return lastSeq; }
    public synchronized long duplicates() { return duplicates; }
    public synchronized long filled() { return filled; }
}
