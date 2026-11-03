package jp.trade.exchange.projection;

/** 投影收到的事件序号不连续。需要上游补数后重试；本批事务已整体回滚。 */
public final class GapException extends RuntimeException {
    public final long expectedSeq;
    public final long actualSeq;

    public GapException(long expectedSeq, long actualSeq) {
        super("event gap: expected seq " + expectedSeq + " but got " + actualSeq);
        this.expectedSeq = expectedSeq;
        this.actualSeq = actualSeq;
    }
}
