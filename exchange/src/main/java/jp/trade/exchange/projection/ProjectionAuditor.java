package jp.trade.exchange.projection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import jp.trade.exchange.core.MatchingEngine;

import static jp.trade.exchange.core.Protocol.*;

/** 稽核：把“确定性重放得到的撮合状态”与 MySQL 投影逐账户、逐订单对账。 */
public final class ProjectionAuditor {
    private ProjectionAuditor() {}

    /** @param skipped 投影尚未追平日志头（命令边界之外对账无意义），未做比较 */
    public record Result(boolean skipped, List<String> diffs) {
        public boolean clean() { return !skipped && diffs.isEmpty(); }
    }

    public static Result audit(Connection c, MatchingEngine engine) throws SQLException {
        final long checkpoint;
        try (PreparedStatement ps = c.prepareStatement("SELECT last_seq FROM projection_checkpoint WHERE id = 1");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            checkpoint = rs.getLong(1);
        }
        final List<String> diffs = new ArrayList<>();
        if (checkpoint != engine.lastEventSeq()) {
            diffs.add("projection lag: checkpoint=" + checkpoint + " engineHead=" + engine.lastEventSeq());
            return new Result(true, diffs);
        }

        // 余额
        final Map<String, long[]> db = new HashMap<>();
        try (PreparedStatement ps = c.prepareStatement("SELECT account_id, asset, available, frozen FROM account_balance");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                db.put(rs.getLong(1) + "/" + rs.getInt(2), new long[]{rs.getLong(3), rs.getLong(4)});
            }
        }
        final Set<String> seen = new HashSet<>();
        engine.forEachAccount((acct, availBase, availQuote, frozenBase, frozenQuote) -> {
            compare(diffs, db, seen, acct, ASSET_BASE, availBase, frozenBase);
            compare(diffs, db, seen, acct, ASSET_QUOTE, availQuote, frozenQuote);
        });
        for (Map.Entry<String, long[]> e : db.entrySet()) {
            if (!seen.contains(e.getKey()) && (e.getValue()[0] != 0 || e.getValue()[1] != 0)) {
                diffs.add("balance only in DB: " + e.getKey());
            }
        }

        // 未成交订单
        final Map<Long, long[]> dbOrders = new HashMap<>();
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT order_id, account_id, side, price, remaining FROM orders WHERE status = " + JdbcProjector.STATUS_OPEN);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                dbOrders.put(rs.getLong(1), new long[]{rs.getLong(2), rs.getInt(3), rs.getLong(4), rs.getLong(5)});
            }
        }
        final Set<Long> engineOrders = new HashSet<>();
        engine.forEachOpenOrder((id, acct, side, price, remaining) -> {
            engineOrders.add(id);
            final long[] d = dbOrders.get(id);
            if (d == null) {
                diffs.add("open order missing in DB: " + id);
            } else if (d[0] != acct || d[1] != side || d[2] != price || d[3] != remaining) {
                diffs.add("order mismatch " + id + ": engine=[" + acct + "," + side + "," + price + "," + remaining
                        + "] db=[" + d[0] + "," + d[1] + "," + d[2] + "," + d[3] + "]");
            }
        });
        for (Long id : dbOrders.keySet()) {
            if (!engineOrders.contains(id)) {
                diffs.add("open order only in DB: " + id);
            }
        }
        return new Result(false, diffs);
    }

    private static void compare(List<String> diffs, Map<String, long[]> db, Set<String> seen,
                                long acct, int asset, long avail, long frozen) {
        final String key = acct + "/" + asset;
        seen.add(key);
        final long[] d = db.getOrDefault(key, new long[]{0, 0});
        if (d[0] != avail || d[1] != frozen) {
            diffs.add("balance mismatch " + key + ": engine=[" + avail + "," + frozen + "] db=[" + d[0] + "," + d[1] + "]");
        }
    }
}
