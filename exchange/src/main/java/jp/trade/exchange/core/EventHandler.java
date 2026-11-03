package jp.trade.exchange.core;

/** 撮合引擎产生事件的回调。引擎在处理命令的线程内同步调用，不得阻塞。 */
@FunctionalInterface
public interface EventHandler {
    void onEvent(int type, int aux, long seq, long ts, long orderId, long account,
                 long price, long qty, long counterOrderId, long counterAccount);
}
