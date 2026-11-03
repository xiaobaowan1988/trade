package jp.trade.exchange.load;

import static jp.trade.exchange.core.Protocol.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * 独立的参考实现，用来做差分验证：只依据设计文档里的语义，用与被测引擎**不同的数据结构**
 * （PriorityQueue + 惰性删除、HashMap，而不是 TreeMap + 同价位队列）重写撮合与记账。
 * 追求的是简单、可读、容易确信它是对的，不追求快。
 *
 * 语义要点：限价 GTC；价格优先、同价时间优先（以订单号即到达顺序为准）；成交价取被动方（maker）价格；
 * 买方按自己限价冻结，成交后退还价格改善；先冻结后撮合；下单/撤单/入金都按键幂等，重试返回原结果。
 */
final class ReferenceExchange {
    record Evt(int type, int aux, long seq, long ts, long orderId, long account, long price, long qty, long cOrder, long cAcct) {}

    private static final class Order {
        final long id, account, price;
        final int side;
        long remaining;
        boolean dead; // 已撤销或已成交（惰性删除）

        Order(long id, long account, int side, long price, long remaining) {
            this.id = id;
            this.account = account;
            this.side = side;
            this.price = price;
            this.remaining = remaining;
        }
    }

    final Map<Long, long[]> avail = new HashMap<>();   // account → {_, base, quote}
    final Map<Long, long[]> frozen = new HashMap<>();
    private final long[] supply = new long[3];
    private final Map<Long, Order> byId = new HashMap<>();
    private final PriorityQueue<Order> bids = new PriorityQueue<>((a, b) -> a.price != b.price ? Long.compare(b.price, a.price) : Long.compare(a.id, b.id));
    private final PriorityQueue<Order> asks = new PriorityQueue<>((a, b) -> a.price != b.price ? Long.compare(a.price, b.price) : Long.compare(a.id, b.id));
    private final Map<String, Long> placeOutcome = new HashMap<>();
    private final Map<String, Long> cancelOutcome = new HashMap<>();
    private final Map<String, Long> depositOutcome = new HashMap<>();
    private long nextOrderId = 1;
    private long nextSeq = 1;
    final List<Evt> events = new ArrayList<>();
    private long now;

    private static String key(long account, long id) { return account + "/" + id; }

    private long[] a(long acct) { return avail.computeIfAbsent(acct, k -> new long[3]); }
    private long[] f(long acct) { return frozen.computeIfAbsent(acct, k -> new long[3]); }

    private void emit(int type, int aux, long orderId, long account, long price, long qty, long cOrder, long cAcct) {
        events.add(new Evt(type, aux, nextSeq++, now, orderId, account, price, qty, cOrder, cAcct));
    }

    void apply(long ts, byte[] cmd) {
        now = ts;
        final java.nio.ByteBuffer b = java.nio.ByteBuffer.wrap(cmd).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        final int type = b.getInt(0);
        final int aux = b.getInt(4);
        final long account = b.getLong(8), f1 = b.getLong(16), f2 = b.getLong(24), f3 = b.getLong(32);
        switch (type) {
            case CMD_DEPOSIT -> deposit(account, aux, f1, f2);
            case CMD_PLACE -> place(account, aux, f1, f2, f3);
            case CMD_CANCEL -> cancel(account, f1);
            default -> throw new IllegalStateException("unexpected command type " + type);
        }
    }

    private void deposit(long acct, int asset, long amount, long depositId) {
        if (amount <= 0 || depositId <= 0 || (asset != ASSET_BASE && asset != ASSET_QUOTE)) {
            emit(EVT_REJECTED, 0, 0, acct, 0, REJECT_INVALID, depositId, 0);
            return;
        }
        final Long prior = depositOutcome.get(key(acct, depositId));
        if (prior != null) {
            emit(EVT_REJECTED, 0, 0, acct, prior, REJECT_DUPLICATE_DEPOSIT, depositId, 0);
            return;
        }
        if (supply[asset] > jp.trade.exchange.core.MatchingEngine.MAX_SUPPLY - amount) {
            emit(EVT_REJECTED, 0, 0, acct, 0, REJECT_OVERFLOW, depositId, 0);
            return;
        }
        a(acct)[asset] += amount;
        supply[asset] += amount;
        depositOutcome.put(key(acct, depositId), amount);
        emit(EVT_DEPOSITED, 0, 0, acct, asset, amount, depositId, 0);
    }

    private void place(long acct, int side, long cid, long price, long qty) {
        if ((side != SIDE_BUY && side != SIDE_SELL) || price <= 0 || qty <= 0) {
            emit(EVT_REJECTED, 0, 0, acct, 0, REJECT_INVALID, cid, 0);
            return;
        }
        final Long prior = placeOutcome.get(key(acct, cid));
        if (prior != null) {
            emit(EVT_REJECTED, 0, prior > 0 ? prior : 0, acct, prior < 0 ? -prior : 0, REJECT_DUPLICATE_CLIENT_ORDER, cid, 0);
            return;
        }
        final long need;
        try {
            need = side == SIDE_BUY ? Math.multiplyExact(price, qty) : qty;
        } catch (ArithmeticException e) {
            placeOutcome.put(key(acct, cid), (long) -REJECT_OVERFLOW);
            emit(EVT_REJECTED, 0, 0, acct, 0, REJECT_OVERFLOW, cid, 0);
            return;
        }
        final int asset = side == SIDE_BUY ? ASSET_QUOTE : ASSET_BASE;
        if (a(acct)[asset] < need) {
            placeOutcome.put(key(acct, cid), (long) -REJECT_INSUFFICIENT_FUNDS);
            emit(EVT_REJECTED, 0, 0, acct, 0, REJECT_INSUFFICIENT_FUNDS, cid, 0);
            return;
        }
        a(acct)[asset] -= need;
        f(acct)[asset] += need;
        final Order taker = new Order(nextOrderId++, acct, side, price, qty);
        placeOutcome.put(key(acct, cid), taker.id);
        emit(EVT_ACCEPTED, side, taker.id, acct, price, qty, cid, 0);

        final PriorityQueue<Order> book = side == SIDE_BUY ? asks : bids;
        while (taker.remaining > 0) {
            Order maker = book.peek();
            while (maker != null && maker.dead) {
                book.poll();
                maker = book.peek();
            }
            if (maker == null) {
                break;
            }
            final boolean crosses = side == SIDE_BUY ? maker.price <= taker.price : maker.price >= taker.price;
            if (!crosses) {
                break;
            }
            final long fill = Math.min(taker.remaining, maker.remaining);
            final Order buy = side == SIDE_BUY ? taker : maker;
            final Order sell = side == SIDE_BUY ? maker : taker;
            final long tradePrice = maker.price;
            f(buy.account)[ASSET_QUOTE] -= buy.price * fill;
            a(buy.account)[ASSET_QUOTE] += (buy.price - tradePrice) * fill;
            a(buy.account)[ASSET_BASE] += fill;
            f(sell.account)[ASSET_BASE] -= fill;
            a(sell.account)[ASSET_QUOTE] += tradePrice * fill;
            taker.remaining -= fill;
            maker.remaining -= fill;
            emit(EVT_TRADE, side, taker.id, taker.account, tradePrice, fill, maker.id, maker.account);
            if (maker.remaining == 0) {
                maker.dead = true;
                byId.remove(maker.id);
                book.poll();
            }
        }
        if (taker.remaining > 0) {
            byId.put(taker.id, taker);
            (side == SIDE_BUY ? bids : asks).add(taker);
        } else {
            taker.dead = true;
        }
    }

    private void cancel(long acct, long orderId) {
        final Order o = byId.get(orderId);
        if (o == null || o.account != acct) {
            final Long prior = cancelOutcome.get(key(acct, orderId));
            if (prior != null) {
                emit(EVT_REJECTED, 0, orderId, acct, prior, REJECT_DUPLICATE_CANCEL, orderId, 0);
            } else {
                emit(EVT_REJECTED, 0, orderId, acct, 0, REJECT_UNKNOWN_ORDER, orderId, 0);
            }
            return;
        }
        o.dead = true;
        byId.remove(orderId);
        cancelOutcome.put(key(acct, orderId), o.remaining);
        if (o.side == SIDE_BUY) {
            final long amt = o.price * o.remaining;
            f(acct)[ASSET_QUOTE] -= amt;
            a(acct)[ASSET_QUOTE] += amt;
        } else {
            f(acct)[ASSET_BASE] -= o.remaining;
            a(acct)[ASSET_BASE] += o.remaining;
        }
        emit(EVT_CANCELLED, o.side, orderId, acct, o.price, o.remaining, 0, 0);
    }

    /** 在簿订单：id → {account, side, price, remaining}。 */
    Map<Long, long[]> openOrders() {
        final Map<Long, long[]> m = new HashMap<>();
        for (Order o : byId.values()) {
            m.put(o.id, new long[]{o.account, o.side, o.price, o.remaining});
        }
        return m;
    }
}
