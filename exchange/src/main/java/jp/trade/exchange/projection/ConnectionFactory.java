package jp.trade.exchange.projection;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface ConnectionFactory {
    Connection get() throws SQLException;
}
