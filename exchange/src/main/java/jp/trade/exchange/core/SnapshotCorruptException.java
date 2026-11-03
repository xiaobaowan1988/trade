package jp.trade.exchange.core;

/** 快照损坏或版本不兼容：拒绝加载。宁可起不来，也不能用一份不可信的状态继续服务。 */
public final class SnapshotCorruptException extends RuntimeException {
    public SnapshotCorruptException(String message) {
        super(message);
    }
}
