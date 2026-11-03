package jp.trade.exchange.events;

import java.io.Closeable;

/**
 * 事件出口：把撮合事件交给下游（Kafka 或日志）。实现必须容忍重复（至少一次语义）。
 *
 * 契约（设计 §0“不能丢”）：publish 之后、flush 成功返回之前，不能认为事件已经送达；
 * flush 必须等到本次 publish 的全部事件被下游确认，任何失败都必须以异常抛出，不能静默忽略。
 */
public interface EventSink extends Closeable {
    /** @param seq 状态机分配的全局事件序号；@param event 72 字节定长事件（调用方不再持有该数组） */
    void publish(long seq, byte[] event);

    /** 阻塞到此前 publish 的事件全部被确认；有任何事件发送失败则抛异常，由调用方对整批重试。 */
    default void flush() {}

    @Override
    default void close() {}
}
