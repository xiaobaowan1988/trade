package jp.trade.exchange.projection;

/**
 * 投影发现数据不自洽（约束被违反、事件类型未知、引用了不存在的订单）。
 * 这不是瞬时故障，重试只会重复同样的错误：必须停下并告警，绝不能继续写入（设计 §0：服务可以挂，但不能出错）。
 */
public final class ProjectionIntegrityException extends RuntimeException {
    public ProjectionIntegrityException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProjectionIntegrityException(String message) {
        super(message);
    }
}
