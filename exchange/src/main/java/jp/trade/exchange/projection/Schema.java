package jp.trade.exchange.projection;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class Schema {
    private Schema() {}

    /** 建表并确保 checkpoint 行存在（幂等）。 */
    public static void apply(Connection c) throws SQLException {
        final String ddl;
        try (InputStream in = Schema.class.getResourceAsStream("/projection/schema.sql")) {
            ddl = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        try (Statement st = c.createStatement()) {
            for (String part : ddl.split(";")) {
                final String sql = part.replaceAll("(?m)^--.*$", "").trim();
                if (!sql.isEmpty()) {
                    st.execute(sql);
                }
            }
            st.execute("INSERT IGNORE INTO projection_checkpoint (id, last_seq) VALUES (1, 0)");
        }
    }

    public static void drop(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            for (String t : new String[]{"trades", "orders", "account_balance", "projection_checkpoint"}) {
                st.execute("DROP TABLE IF EXISTS " + t);
            }
        }
    }
}
