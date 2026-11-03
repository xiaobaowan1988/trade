package jp.trade.exchange.core;

import static jp.trade.exchange.core.Protocol.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.agrona.DirectBuffer;

/**
 * 确定性单线程撮合状态机（单市场、限价 GTC、价格-时间优先）。
 *
 * 确定性约束：
 *  - 不读系统时间：时间戳由调用方（集群日志时间）传入；
 *  - 不用随机数、不用浮点；
 *  - 所有会被迭代的集合都是有序的（TreeMap/TreeSet/ArrayDeque）；HashMap 只做点查，从不迭代；
 *  - orderId 与 eventSeq 由状态机自增，随快照持久化。
 *
 * 资金在撮合前冻结：买单冻结 price*qty 的 quote，卖单冻结 qty 的 base。
 */
public final class MatchingEngine {

    private static final class Account {
        final long[] available = new long[3]; // 下标为 asset：1=base，2=quote
        final long[] frozen = new long[3];
    }

    private static final class Order {
        final long id;
        final long account;
        final int side;
        final long price;
        long remaining;

        Order(long id, long account, int side, long price, long remaining) {
            this.id = id;
            this.account = account;
            this.side = side;
            this.price = price;
            this.remaining = remaining;
        }
    }

    private record ClientKey(long account, long clientOrderId) implements Comparable<ClientKey> {
        @Override
        public int compareTo(ClientKey o) {
            final int c = Long.compare(account, o.account);
            return c != 0 ? c : Long.compare(clientOrderId, o.clientOrderId);
        }
    }

    private final TreeMap<Long, Account> accounts = new TreeMap<>();
    private final TreeMap<Long, ArrayDeque<Order>> bids = new TreeMap<>(Comparator.reverseOrder());
    private final TreeMap<Long, ArrayDeque<Order>> asks = new TreeMap<>();
    private final Map<Long, Order> ordersById = new HashMap<>(); // 仅点查
    private final TreeSet<ClientKey> seenClientOrders = new TreeSet<>();
    private long nextOrderId = 1;
    private long nextEventSeq = 1;

    private EventHandler out;
    private long now;

    /** 处理一条命令。events 在同一线程内同步回调。 */
    public void onCommand(DirectBuffer buf, int offset, int length, long timestamp, EventHandler events) {
        this.out = events;
        this.now = timestamp;
        if (length != COMMAND_LENGTH) {
            emit(EVT_REJECTED, 0, 0, 0, 0, REJECT_MALFORMED, 0, 0);
            return;
        }
        final int type = buf.getInt(offset);
        final int aux = buf.getInt(offset + 4);
        final long account = buf.getLong(offset + 8);
        final long f1 = buf.getLong(offset + 16);
        final long f2 = buf.getLong(offset + 24);
        final long f3 = buf.getLong(offset + 32);
        switch (type) {
            case CMD_DEPOSIT -> deposit(account, aux, f1);
            case CMD_PLACE -> place(account, aux, f1, f2, f3);
            case CMD_CANCEL -> cancel(account, f1);
            default -> emit(EVT_REJECTED, 0, 0, account, 0, REJECT_MALFORMED, 0, 0);
        }
    }

    private void deposit(long accountId, int asset, long amount) {
        if (amount <= 0 || (asset != ASSET_BASE && asset != ASSET_QUOTE)) {
            emit(EVT_REJECTED, 0, 0, accountId, 0, REJECT_INVALID, 0, 0);
            return;
        }
        final Account a = accounts.computeIfAbsent(accountId, k -> new Account());
        try {
            a.available[asset] = Math.addExact(a.available[asset], amount);
        } catch (ArithmeticException e) {
            emit(EVT_REJECTED, 0, 0, accountId, 0, REJECT_OVERFLOW, 0, 0);
            return;
        }
        emit(EVT_DEPOSITED, 0, 0, accountId, asset, amount, 0, 0);
    }

    private void place(long accountId, int side, long clientOrderId, long price, long qty) {
        if ((side != SIDE_BUY && side != SIDE_SELL) || price <= 0 || qty <= 0) {
            emit(EVT_REJECTED, 0, 0, accountId, 0, REJECT_INVALID, clientOrderId, 0);
            return;
        }
        if (!seenClientOrders.add(new ClientKey(accountId, clientOrderId))) {
            emit(EVT_REJECTED, 0, 0, accountId, 0, REJECT_DUPLICATE_CLIENT_ORDER, clientOrderId, 0);
            return;
        }
        final Account acct = accounts.get(accountId);
        final int freezeAsset = side == SIDE_BUY ? ASSET_QUOTE : ASSET_BASE;
        final long freezeAmount;
        try {
            freezeAmount = side == SIDE_BUY ? Math.multiplyExact(price, qty) : qty;
        } catch (ArithmeticException e) {
            emit(EVT_REJECTED, 0, 0, accountId, 0, REJECT_OVERFLOW, clientOrderId, 0);
            return;
        }
        if (acct == null || acct.available[freezeAsset] < freezeAmount) {
            emit(EVT_REJECTED, 0, 0, accountId, 0, REJECT_INSUFFICIENT_FUNDS, clientOrderId, 0);
            return;
        }
        // 先冻结，后撮合
        acct.available[freezeAsset] -= freezeAmount;
        acct.frozen[freezeAsset] += freezeAmount;

        final Order taker = new Order(nextOrderId++, accountId, side, price, qty);
        emit(EVT_ACCEPTED, side, taker.id, accountId, price, qty, clientOrderId, 0);

        match(taker);

        if (taker.remaining > 0) {
            book(side).computeIfAbsent(price, k -> new ArrayDeque<>()).addLast(taker);
            ordersById.put(taker.id, taker);
        }
    }

    private TreeMap<Long, ArrayDeque<Order>> book(int side) {
        return side == SIDE_BUY ? bids : asks;
    }

    private void match(Order taker) {
        final TreeMap<Long, ArrayDeque<Order>> opposite = book(taker.side == SIDE_BUY ? SIDE_SELL : SIDE_BUY);
        while (taker.remaining > 0 && !opposite.isEmpty()) {
            final Map.Entry<Long, ArrayDeque<Order>> best = opposite.firstEntry();
            final long levelPrice = best.getKey();
            final boolean crosses = taker.side == SIDE_BUY ? levelPrice <= taker.price : levelPrice >= taker.price;
            if (!crosses) {
                break;
            }
            final ArrayDeque<Order> level = best.getValue();
            while (taker.remaining > 0 && !level.isEmpty()) {
                final Order maker = level.peekFirst();
                final long fill = Math.min(taker.remaining, maker.remaining);
                final Order buy = taker.side == SIDE_BUY ? taker : maker;
                final Order sell = taker.side == SIDE_BUY ? maker : taker;
                settle(buy, sell, levelPrice, fill);
                taker.remaining -= fill;
                maker.remaining -= fill;
                emit(EVT_TRADE, taker.side, taker.id, taker.account, levelPrice, fill, maker.id, maker.account);
                if (maker.remaining == 0) {
                    level.pollFirst();
                    ordersById.remove(maker.id);
                }
            }
            if (level.isEmpty()) {
                opposite.remove(levelPrice);
            }
        }
    }

    /** 以 maker 的价格成交；买方按自己限价冻结的差额退回（价格改善）。 */
    private void settle(Order buy, Order sell, long tradePrice, long fill) {
        final Account b = accounts.get(buy.account);
        final Account s = accounts.get(sell.account);
        b.frozen[ASSET_QUOTE] -= buy.price * fill;                       // 释放按限价冻结的部分
        b.available[ASSET_QUOTE] += (buy.price - tradePrice) * fill;     // 退还价格改善
        b.available[ASSET_BASE] += fill;
        s.frozen[ASSET_BASE] -= fill;
        s.available[ASSET_QUOTE] += tradePrice * fill;
    }

    private void cancel(long accountId, long orderId) {
        final Order o = ordersById.get(orderId);
        if (o == null || o.account != accountId) {
            emit(EVT_REJECTED, 0, orderId, accountId, 0, REJECT_UNKNOWN_ORDER, orderId, 0);
            return;
        }
        final TreeMap<Long, ArrayDeque<Order>> bk = book(o.side);
        final ArrayDeque<Order> level = bk.get(o.price);
        level.remove(o);
        if (level.isEmpty()) {
            bk.remove(o.price);
        }
        ordersById.remove(orderId);
        final Account a = accounts.get(accountId);
        if (o.side == SIDE_BUY) {
            final long amt = o.price * o.remaining;
            a.frozen[ASSET_QUOTE] -= amt;
            a.available[ASSET_QUOTE] += amt;
        } else {
            a.frozen[ASSET_BASE] -= o.remaining;
            a.available[ASSET_BASE] += o.remaining;
        }
        emit(EVT_CANCELLED, o.side, o.id, accountId, o.price, o.remaining, 0, 0);
    }

    private void emit(int type, int aux, long orderId, long account, long price, long qty, long counterOrderId, long counterAccount) {
        // 参数顺序对 REJECTED/DEPOSITED 做了便捷复用：见 Protocol 注释
        out.onEvent(type, aux, nextEventSeq++, now, orderId, account, price, qty, counterOrderId, counterAccount);
    }

    // ---------- 只读访问（测试 / 运维） ----------

    public long available(long account, int asset) {
        final Account a = accounts.get(account);
        return a == null ? 0 : a.available[asset];
    }

    public long frozen(long account, int asset) {
        final Account a = accounts.get(account);
        return a == null ? 0 : a.frozen[asset];
    }

    public long totalHeld(int asset) {
        long t = 0;
        for (Account a : accounts.values()) {
            t += a.available[asset] + a.frozen[asset];
        }
        return t;
    }

    public long lastEventSeq() { return nextEventSeq - 1; }

    public int restingOrderCount() { return ordersById.size(); }

    public Long bestBid() { return bids.isEmpty() ? null : bids.firstKey(); }

    public Long bestAsk() { return asks.isEmpty() ? null : asks.firstKey(); }

    // ---------- 快照 ----------

    /** 序列化完整状态。遍历顺序全部有序，因此同一状态产生的字节完全一致，可直接用作状态哈希的输入。 */
    public byte[] snapshot() {
        int orderCount = ordersById.size();
        final ByteBuffer bb = ByteBuffer.allocate(
                8 * 2 + 4 + accounts.size() * (8 + 8 * 4) + 4 + orderCount * (8 * 4 + 4) + 4 + seenClientOrders.size() * 16)
                .order(ByteOrder.LITTLE_ENDIAN);
        bb.putLong(nextOrderId);
        bb.putLong(nextEventSeq);
        bb.putInt(accounts.size());
        for (Map.Entry<Long, Account> e : accounts.entrySet()) {
            bb.putLong(e.getKey());
            final Account a = e.getValue();
            bb.putLong(a.available[1]).putLong(a.available[2]).putLong(a.frozen[1]).putLong(a.frozen[2]);
        }
        bb.putInt(orderCount);
        // 先买盘（价格从高到低）后卖盘（从低到高），同价位按时间顺序；恢复时按序追加即可还原时间优先
        for (int side : new int[]{SIDE_BUY, SIDE_SELL}) {
            for (ArrayDeque<Order> level : book(side).values()) {
                for (Order o : level) {
                    bb.putLong(o.id).putLong(o.account).putLong(o.price).putLong(o.remaining).putInt(o.side);
                }
            }
        }
        bb.putInt(seenClientOrders.size());
        for (ClientKey k : seenClientOrders) {
            bb.putLong(k.account).putLong(k.clientOrderId);
        }
        return bb.array();
    }

    public static MatchingEngine fromSnapshot(byte[] data) {
        final ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        final MatchingEngine e = new MatchingEngine();
        e.nextOrderId = bb.getLong();
        e.nextEventSeq = bb.getLong();
        int n = bb.getInt();
        for (int i = 0; i < n; i++) {
            final long id = bb.getLong();
            final Account a = new Account();
            a.available[1] = bb.getLong();
            a.available[2] = bb.getLong();
            a.frozen[1] = bb.getLong();
            a.frozen[2] = bb.getLong();
            e.accounts.put(id, a);
        }
        n = bb.getInt();
        for (int i = 0; i < n; i++) {
            final long id = bb.getLong();
            final long account = bb.getLong();
            final long price = bb.getLong();
            final long remaining = bb.getLong();
            final int side = bb.getInt();
            final Order o = new Order(id, account, side, price, remaining);
            e.book(side).computeIfAbsent(price, k -> new ArrayDeque<>()).addLast(o);
            e.ordersById.put(id, o);
        }
        n = bb.getInt();
        for (int i = 0; i < n; i++) {
            e.seenClientOrders.add(new ClientKey(bb.getLong(), bb.getLong()));
        }
        return e;
    }

    public String stateHash() {
        try {
            final byte[] d = MessageDigest.getInstance("SHA-256").digest(snapshot());
            final StringBuilder sb = new StringBuilder();
            for (byte b : d) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
