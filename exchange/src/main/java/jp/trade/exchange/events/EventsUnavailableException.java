package jp.trade.exchange.events;

/**
 * 请求的事件已无法由本地 Raft 日志/快照重新生成（日志已裁剪）。
 * 这类缺口只能从 S3 归档恢复，属于运维事件。
 */
public final class EventsUnavailableException extends RuntimeException {
    public final long requestedFromSeq;
    /** 本地仍可补数的最小事件序号（Long.MAX_VALUE 表示本地没有任何可用种子）。 */
    public final long earliestRecoverableSeq;

    public EventsUnavailableException(long requestedFromSeq, long earliestRecoverableSeq) {
        super("events from seq " + requestedFromSeq + " cannot be regenerated locally; earliest recoverable seq is "
                + earliestRecoverableSeq + " (restore from archive)");
        this.requestedFromSeq = requestedFromSeq;
        this.earliestRecoverableSeq = earliestRecoverableSeq;
    }
}
