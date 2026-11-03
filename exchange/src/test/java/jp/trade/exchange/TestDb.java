package jp.trade.exchange;

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import jp.trade.exchange.projection.ConnectionFactory;
import jp.trade.exchange.projection.Schema;

/** 整个测试 JVM 共享一个真实 MariaDB 进程（MySQL 兼容）；每个测试重建表。 */
public final class TestDb {
    private static DBConfigurationBuilder cfg;
    private static DB db;

    private TestDb() {}

    public static synchronized ConnectionFactory factory() throws Exception {
        if (db == null) {
            cfg = DBConfigurationBuilder.newBuilder();
            cfg.setPort(0);
            cfg.addArg("--user=root");
            db = DB.newEmbeddedDB(cfg.build());
            db.start();
            db.createDB("exch");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    db.stop();
                } catch (Exception ignored) {
                }
            }));
        }
        final String url = cfg.getURL("exch");
        return () -> DriverManager.getConnection(url, "root", "");
    }

    public static ConnectionFactory freshSchema() throws Exception {
        final ConnectionFactory f = factory();
        try (Connection c = f.get()) {
            Schema.drop(c);
            Schema.apply(c);
        }
        return f;
    }

    /** 所有投影表内容的稳定指纹，用于比较两个库状态是否完全相同。 */
    static String fingerprint(ConnectionFactory f) throws SQLException {
        final List<String> rows = new ArrayList<>();
        try (Connection c = f.get(); Statement st = c.createStatement()) {
            for (String q : new String[]{
                    "SELECT 'cp', id, last_seq FROM projection_checkpoint ORDER BY id",
                    "SELECT 'bal', account_id, asset, available, frozen FROM account_balance ORDER BY account_id, asset",
                    "SELECT 'ord', order_id, account_id, side, price, qty, remaining, status, client_order_id, created_ts, updated_seq FROM orders ORDER BY order_id",
                    "SELECT 'trd', seq, taker_order_id, maker_order_id, taker_account, maker_account, taker_side, price, qty, ts FROM trades ORDER BY seq"}) {
                try (ResultSet rs = st.executeQuery(q)) {
                    final int n = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        final StringBuilder sb = new StringBuilder();
                        for (int i = 1; i <= n; i++) {
                            sb.append(rs.getString(i)).append('|');
                        }
                        rows.add(sb.toString());
                    }
                }
            }
        }
        return String.join("\n", rows);
    }

    static long count(ConnectionFactory f, String table) throws SQLException {
        try (Connection c = f.get(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
