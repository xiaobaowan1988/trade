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
                final Batch batch = new Batch(c);
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
                        applyEvent(batch, b);
                        cur = seq;
                        afterEventHook.accept(seq);
                    }
                    batch.flush(); // 整批内存累计完成后，用批量语句一次写入
                } finally {
                    batch.close();
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
            if (isIntegrityViolation(e)) {
                throw new ProjectionIntegrityException("projection rejected an inconsistent batch: " + e.getMessage(), e);
            }
            throw new IllegalStateException(e);
        }
    }

    /** SQLState 23xxx（完整性约束，含 CHECK、主键重复）：不是瞬时故障，不能重试。 */
    private static boolean isIntegrityViolation(SQLException e) {
        for (SQLException x = e; x != null; x = x.getNextException()) {
            if (x instanceof java.sql.SQLIntegrityConstraintViolationException
                    || (x.getSQLState() != null && x.getSQLState().startsWith("23"))) {
                return true;
            }
        }
        return false;
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

    private void applyEvent(Batch x, UnsafeBuffer b) throws SQLException {
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
            default -> throw new ProjectionIntegrityException("unknown event type " + type + " (state machine version mismatch?)");
        }
    }

    /**
     * 一个批次内的累计：余额增量、订单变化、成交都先在内存里合并，批末用批量语句一次写入。
     * 逐事件执行约 10 条 SQL 的做法吞吐只有每秒两千多事件；合并后每批只剩“每个受影响账户/订单一条”。
     * 语义与逐事件应用完全相同（同一事务、同一终态），只是少了往返。
     */
    private static final class Batch implements AutoCloseable {
        private static final class OrderAcc {
            boolean isNew;
            long account, price, qty, clientId, createdTs;
            int side;
            long fills;
            boolean cancelled;
            long lastSeq;
        }

        private final Connection c;
        private final java.util.TreeMap<Long, long[]> balances = new java.util.TreeMap<>(); // key=account*4+asset → {dAvail, dFrozen}
        private final java.util.TreeMap<Long, OrderAcc> orders = new java.util.TreeMap<>();
        private final List<long[]> trades = new java.util.ArrayList<>();
        private final PreparedStatement orderPrice;

        Batch(Connection c) throws SQLException {
            this.c = c;
            this.orderPrice = c.prepareStatement("SELECT price FROM orders WHERE order_id = ?");
        }

        void balance(long account, int asset, long dAvailable, long dFrozen) {
            if (dAvailable == 0 && dFrozen == 0) {
                return;
            }
            final long[] d = balances.computeIfAbsent(account * 4 + asset, k -> new long[2]);
            d[0] += dAvailable;
            d[1] += dFrozen;
        }

        void insertOrder(long id, long account, int side, long price, long qty, long clientId, long ts, long seq) {
            final OrderAcc o = new OrderAcc();
            o.isNew = true;
            o.account = account;
            o.side = side;
            o.price = price;
            o.qty = qty;
            o.clientId = clientId;
            o.createdTs = ts;
            o.lastSeq = seq;
            orders.put(id, o);
        }

        /** 买方限价：本批内新建的订单直接取内存，否则查库（ACCEPTED 与 TRADE 被分在两个批次时）。 */
        long orderPrice(long orderId) throws SQLException {
            final OrderAcc o = orders.get(orderId);
            if (o != null && o.isNew) {
                return o.price;
            }
            orderPrice.setLong(1, orderId);
            try (ResultSet rs = orderPrice.executeQuery()) {
                if (!rs.next()) {
                    throw new ProjectionIntegrityException("order " + orderId + " missing in projection");
                }
                return rs.getLong(1);
            }
        }

        void fill(long orderId, long qty, long seq) {
            final OrderAcc o = orders.computeIfAbsent(orderId, k -> new OrderAcc());
            o.fills += qty;
            o.lastSeq = seq;
        }

        void cancel(long orderId, long seq) {
            final OrderAcc o = orders.computeIfAbsent(orderId, k -> new OrderAcc());
            o.cancelled = true;
            o.lastSeq = seq;
        }

        void insertTrade(long seq, long taker, long maker, long takerAcct, long makerAcct, int takerSide,
                         long price, long qty, long ts) {
            trades.add(new long[]{seq, taker, maker, takerAcct, makerAcct, takerSide, price, qty, ts});
        }

        void flush() throws SQLException {
            // 1. 余额：先确保行存在（INSERT IGNORE 零行），再对每个受影响的 (账户, 资产) 加一条增量。
            //    不能用 INSERT ... ON DUPLICATE KEY UPDATE 直接插增量：MariaDB/MySQL 会先对“插入元组”检查 CHECK，
            //    而增量常常是负的（比如冻结 500 就是 available = -500），即使合并后的结果是合法的也会被拒绝。
            //    这样 CHECK（余额非负）只作用于最终结果，作为数据库的最后一道防线。
            try (PreparedStatement ensure = c.prepareStatement(
                         "INSERT IGNORE INTO account_balance (account_id, asset, available, frozen) VALUES (?, ?, 0, 0)");
                 PreparedStatement add = c.prepareStatement(
                         "UPDATE account_balance SET available = available + ?, frozen = frozen + ? WHERE account_id = ? AND asset = ?")) {
                for (java.util.Map.Entry<Long, long[]> e : balances.entrySet()) {
                    ensure.setLong(1, e.getKey() / 4);
                    ensure.setInt(2, (int) (e.getKey() % 4));
                    ensure.addBatch();
                    add.setLong(1, e.getValue()[0]);
                    add.setLong(2, e.getValue()[1]);
                    add.setLong(3, e.getKey() / 4);
                    add.setInt(4, (int) (e.getKey() % 4));
                    add.addBatch();
                }
                ensure.executeBatch();
                add.executeBatch();
            }
            // 2. 订单：本批新建的一次插入（带最终状态）；已存在的按增量更新
            try (PreparedStatement ins = c.prepareStatement(
                         "INSERT INTO orders (order_id, account_id, side, price, qty, remaining, status, client_order_id, created_ts, updated_seq) "
                                 + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                 PreparedStatement cancel = c.prepareStatement(
                         "UPDATE orders SET status = 2, remaining = 0, updated_seq = ? WHERE order_id = ?");
                 PreparedStatement fill = c.prepareStatement(
                         "UPDATE orders SET remaining = remaining - ?, updated_seq = ? WHERE order_id = ?");
                 PreparedStatement filled = c.prepareStatement(
                         "UPDATE orders SET status = 1 WHERE order_id = ? AND remaining = 0 AND status = 0")) {
                for (java.util.Map.Entry<Long, OrderAcc> e : orders.entrySet()) {
                    final long id = e.getKey();
                    final OrderAcc o = e.getValue();
                    if (o.isNew) {
                        final long remaining = o.cancelled ? 0 : o.qty - o.fills;
                        final int status = o.cancelled ? STATUS_CANCELLED : (remaining == 0 ? STATUS_FILLED : STATUS_OPEN);
                        ins.setLong(1, id);
                        ins.setLong(2, o.account);
                        ins.setInt(3, o.side);
                        ins.setLong(4, o.price);
                        ins.setLong(5, o.qty);
                        ins.setLong(6, remaining);
                        ins.setInt(7, status);
                        ins.setLong(8, o.clientId);
                        ins.setLong(9, o.createdTs);
                        ins.setLong(10, o.lastSeq);
                        ins.addBatch();
                    } else if (o.cancelled) {
                        cancel.setLong(1, o.lastSeq);
                        cancel.setLong(2, id);
                        cancel.addBatch();
                    } else {
                        fill.setLong(1, o.fills);
                        fill.setLong(2, o.lastSeq);
                        fill.setLong(3, id);
                        fill.addBatch();
                        filled.setLong(1, id);
                        filled.addBatch();
                    }
                }
                ins.executeBatch();
                cancel.executeBatch();
                fill.executeBatch();
                filled.executeBatch(); // 必须在 fill 之后：此时 remaining 已是最终值
            }
            // 3. 成交
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO trades (seq, taker_order_id, maker_order_id, taker_account, maker_account, taker_side, price, qty, ts) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                for (long[] t : trades) {
                    for (int i = 0; i < t.length; i++) {
                        ps.setLong(i + 1, t[i]);
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }

        @Override
        public void close() throws SQLException {
            orderPrice.close();
        }
    }
}
