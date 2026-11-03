package jp.trade.exchange.projection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.LongConsumer;
import org.agrona.concurrent.UnsafeBuffer;

import static jp.trade.exchange.core.Protocol.*;

/**
 * 事件 → MySQL 投影。恰好一次靠“同一事务”：
 * 一批事件的业务写入与 projection_checkpoint.last_seq 一起提交；事务内先 SELECT ... FOR UPDATE
 * 锁住 checkpoint（顺带防止两个投影进程同时写）。
 *   seq <= last_seq  → 重复，跳过（幂等）
 *   seq == last_seq+1 → 应用
 *   否则              → 缺口，整批回滚并抛 {@link GapException}
 *
 * 资金公式与撮合引擎一致，且不回查撮合内存：只靠事件和 orders 表里保存的限价。
 */
public final class JdbcProjector {
    public static final int STATUS_OPEN = 0;
    public static final int STATUS_FILLED = 1;
    public static final int STATUS_CANCELLED = 2;

    private final ConnectionFactory connections;
    /** 仅测试：每应用一条事件后回调，可抛异常模拟崩溃。 */
    private LongConsumer afterEventHook = seq -> {};

    public JdbcProjector(ConnectionFactory connections) {
        this.connections = connections;
    }

    void setAfterEventHook(LongConsumer hook) { this.afterEventHook = hook; }

    public long checkpoint() {
        try (Connection c = connections.get();
             PreparedStatement ps = c.prepareStatement("SELECT last_seq FROM projection_checkpoint WHERE id = 1");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 在一个事务里应用一批事件，返回提交后的 last_seq。 */
    public long apply(List<byte[]> events) {
        try (Connection c = connections.get()) {
            c.setAutoCommit(false);
            try {
                final long before = lockCheckpoint(c);
                long cur = before;
                final Ctx ctx = new Ctx(c);
                try {
                    for (byte[] e : events) {
                        final UnsafeBuffer b = new UnsafeBuffer(e);
                        final long seq = eventSeq(b, 0);
                        if (seq <= cur) {
                            continue;
                        }
                        if (seq != cur + 1) {
                            throw new GapException(cur + 1, seq);
                        }
                        applyEvent(ctx, b);
                        cur = seq;
                        afterEventHook.accept(seq);
                    }
                } finally {
                    ctx.close();
                }
                if (cur > before) {
                    try (PreparedStatement ps = c.prepareStatement("UPDATE projection_checkpoint SET last_seq = ? WHERE id = 1")) {
                        ps.setLong(1, cur);
                        ps.executeUpdate();
                    }
                }
                c.commit();
                return cur;
            } catch (Throwable t) {
                c.rollback();
                throw t;
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static long lockCheckpoint(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT last_seq FROM projection_checkpoint WHERE id = 1 FOR UPDATE");
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                throw new IllegalStateException("projection schema not initialised");
            }
            return rs.getLong(1);
        }
    }

    // ---------- 单事件应用 ----------

    private void applyEvent(Ctx x, UnsafeBuffer b) throws SQLException {
        final int type = eventType(b, 0);
        final int aux = eventAux(b, 0);
        final long seq = eventSeq(b, 0);
        final long ts = eventTimestamp(b, 0);
        final long orderId = eventOrderId(b, 0);
        final long account = eventAccount(b, 0);
        final long price = eventPrice(b, 0);
        final long qty = eventQty(b, 0);
        final long counterOrderId = eventCounterOrderId(b, 0);
        final long counterAccount = eventCounterAccount(b, 0);

        switch (type) {
            case EVT_DEPOSITED -> x.balance(account, (int) price, qty, 0);           // price 字段存 asset
            case EVT_ACCEPTED -> {                                                    // aux=side, counterOrderId=clientOrderId
                x.insertOrder(orderId, account, aux, price, qty, counterOrderId, ts, seq);
                if (aux == SIDE_BUY) {
                    x.balance(account, ASSET_QUOTE, -price * qty, price * qty);
                } else {
                    x.balance(account, ASSET_BASE, -qty, qty);
                }
            }
            case EVT_TRADE -> {                                                       // aux=takerSide, qty=成交量, price=成交价
                x.insertTrade(seq, orderId, counterOrderId, account, counterAccount, aux, price, qty, ts);
                final long buyAccount = aux == SIDE_BUY ? account : counterAccount;
                final long sellAccount = aux == SIDE_BUY ? counterAccount : account;
                // 买方限价：taker 是买方时要查 orders；maker 是买方时限价就等于成交价
                final long buyLimit = aux == SIDE_BUY ? x.orderPrice(orderId) : price;
                x.balance(buyAccount, ASSET_QUOTE, (buyLimit - price) * qty, -buyLimit * qty);
                x.balance(buyAccount, ASSET_BASE, qty, 0);
                x.balance(sellAccount, ASSET_BASE, 0, -qty);
                x.balance(sellAccount, ASSET_QUOTE, price * qty, 0);
                x.fill(orderId, qty, seq);
                x.fill(counterOrderId, qty, seq);
            }
            case EVT_CANCELLED -> {                                                   // aux=side, price=限价, qty=剩余
                x.cancel(orderId, seq);
                if (aux == SIDE_BUY) {
                    x.balance(account, ASSET_QUOTE, price * qty, -price * qty);
                } else {
                    x.balance(account, ASSET_BASE, qty, -qty);
                }
            }
            case EVT_REJECTED -> { /* 无状态变化，只推进 checkpoint */ }
            default -> throw new IllegalStateException("unknown event type " + type);
        }
    }

    /** 一个事务内复用的预编译语句。 */
    private static final class Ctx implements AutoCloseable {
        private final PreparedStatement balance;
        private final PreparedStatement insertOrder;
        private final PreparedStatement orderPrice;
        private final PreparedStatement fillRemaining;
        private final PreparedStatement fillStatus;
        private final PreparedStatement cancel;
        private final PreparedStatement insertTrade;

        Ctx(Connection c) throws SQLException {
            balance = c.prepareStatement(
                    "INSERT INTO account_balance (account_id, asset, available, frozen) VALUES (?, ?, ?, ?) "
                            + "ON DUPLICATE KEY UPDATE available = available + VALUES(available), frozen = frozen + VALUES(frozen)");
            insertOrder = c.prepareStatement(
                    "INSERT INTO orders (order_id, account_id, side, price, qty, remaining, status, client_order_id, created_ts, updated_seq) "
                            + "VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)");
            orderPrice = c.prepareStatement("SELECT price FROM orders WHERE order_id = ?");
            fillRemaining = c.prepareStatement("UPDATE orders SET remaining = remaining - ?, updated_seq = ? WHERE order_id = ?");
            fillStatus = c.prepareStatement("UPDATE orders SET status = 1 WHERE order_id = ? AND remaining = 0");
            cancel = c.prepareStatement("UPDATE orders SET status = 2, remaining = 0, updated_seq = ? WHERE order_id = ?");
            insertTrade = c.prepareStatement(
                    "INSERT INTO trades (seq, taker_order_id, maker_order_id, taker_account, maker_account, taker_side, price, qty, ts) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        }

        void balance(long account, int asset, long dAvailable, long dFrozen) throws SQLException {
            if (dAvailable == 0 && dFrozen == 0) {
                return;
            }
            balance.setLong(1, account);
            balance.setInt(2, asset);
            balance.setLong(3, dAvailable);
            balance.setLong(4, dFrozen);
            balance.executeUpdate();
        }

        void insertOrder(long id, long account, int side, long price, long qty, long clientId, long ts, long seq) throws SQLException {
            insertOrder.setLong(1, id);
            insertOrder.setLong(2, account);
            insertOrder.setInt(3, side);
            insertOrder.setLong(4, price);
            insertOrder.setLong(5, qty);
            insertOrder.setLong(6, qty);
            insertOrder.setLong(7, clientId);
            insertOrder.setLong(8, ts);
            insertOrder.setLong(9, seq);
            insertOrder.executeUpdate();
        }

        long orderPrice(long orderId) throws SQLException {
            orderPrice.setLong(1, orderId);
            try (ResultSet rs = orderPrice.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("order " + orderId + " missing in projection");
                }
                return rs.getLong(1);
            }
        }

        void fill(long orderId, long qty, long seq) throws SQLException {
            fillRemaining.setLong(1, qty);
            fillRemaining.setLong(2, seq);
            fillRemaining.setLong(3, orderId);
            fillRemaining.executeUpdate();
            fillStatus.setLong(1, orderId);
            fillStatus.executeUpdate();
        }

        void cancel(long orderId, long seq) throws SQLException {
            cancel.setLong(1, seq);
            cancel.setLong(2, orderId);
            cancel.executeUpdate();
        }

        void insertTrade(long seq, long taker, long maker, long takerAcct, long makerAcct, int takerSide,
                         long price, long qty, long ts) throws SQLException {
            insertTrade.setLong(1, seq);
            insertTrade.setLong(2, taker);
            insertTrade.setLong(3, maker);
            insertTrade.setLong(4, takerAcct);
            insertTrade.setLong(5, makerAcct);
            insertTrade.setInt(6, takerSide);
            insertTrade.setLong(7, price);
            insertTrade.setLong(8, qty);
            insertTrade.setLong(9, ts);
            insertTrade.executeUpdate();
        }

        @Override
        public void close() throws SQLException {
            for (PreparedStatement ps : new PreparedStatement[]{balance, insertOrder, orderPrice, fillRemaining, fillStatus, cancel, insertTrade}) {
                ps.close();
            }
        }
    }
}
