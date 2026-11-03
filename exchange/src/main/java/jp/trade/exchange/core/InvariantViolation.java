package jp.trade.exchange.core;

/** 撮合状态违反了不变量（余额为负、守恒被破坏等）。这是程序缺陷，必须 fail-stop，绝不能继续服务。 */
public final class InvariantViolation extends IllegalStateException {
    public InvariantViolation(String message) {
        super(message);
    }
}
