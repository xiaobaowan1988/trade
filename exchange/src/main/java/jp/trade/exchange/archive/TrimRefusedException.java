package jp.trade.exchange.archive;

/** 日志裁剪被拒绝：对应的数据尚未可靠地归档到对象存储。 */
public final class TrimRefusedException extends RuntimeException {
    public TrimRefusedException(String reason) {
        super("log trim refused: " + reason);
    }
}
