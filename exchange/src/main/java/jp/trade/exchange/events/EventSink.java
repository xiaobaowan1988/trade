package jp.trade.exchange.events;

import java.io.Closeable;

/** 事件出口：把撮合事件交给下游（Kafka 或日志）。实现必须容忍重复（至少一次语义）。 */
public interface EventSink extends Closeable {
    /** @param seq 状态机分配的全局事件序号；@param event 72 字节定长事件（调用方不再持有该数组） */
    void publish(long seq, byte[] event);

    @Override
    default void close() {}
}
