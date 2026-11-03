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

        /** 受检地同时改变可用与冻结。任何一边溢出或变负都是程序缺陷：先算后赋值，不留下半截状态。 */
        void apply(int asset, long dAvailable, long dFrozen) {
            final long na = Math.addExact(available[asset], dAvailable);
            final long nf = Math.addExact(frozen[asset], dFrozen);
            if (na < 0 || nf < 0) {
                throw new InvariantViolation("negative balance: asset=" + asset + " available " + available[asset] + "→" + na
                        + ", frozen " + frozen[asset] + "→" + nf);
            }
            available[asset] = na;
            frozen[asset] = nf;
        }
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
    /**
     * 每个 (账户, clientOrderId) 的结果：>0 是接受后的订单号，<0 是被拒原因的负数。
     * 重试同一 clientOrderId 时据此返回原结果，客户端丢了回报也能取回（设计 §1）。
     */
    private final TreeMap<ClientKey, Long> placeOutcomes = new TreeMap<>();
    /** 已撤销订单的结果：key=(账户, 订单号)，value=撤销时的剩余数量。重复撤单据此返回原结果。 */
    private final TreeMap<ClientKey, Long> cancelOutcomes = new TreeMap<>();
    /** 已入账的入金：key=(账户, depositId)，value=金额。重试同一笔入金时返回原结果，绝不重复入账。 */
    private final TreeMap<ClientKey, Long> depositOutcomes = new TreeMap<>();
    /** 每种资产总供应量上限：保证任何账户余额之和都不会让 long 溢出（设计 §0 第 1 点：先校验、后改状态）。 */
    public static final long MAX_SUPPLY = Long.MAX_VALUE / 4;
    /** 快照格式：魔数 + 版本 + 正文 + SHA-256。 */
    private static final int SNAPSHOT_MAGIC = 0x4E58454E; // "NEXN"
    public static final int SNAPSHOT_VERSION = 3;
    /** 每多少个事件产生一个链式哈希检查点（属于状态机语义，改它要升版本）。 */
    public static final long CHECKPOINT_INTERVAL = 1_000;
    private final long[] supply = new long[3]; // 已入账的总额，下标为 asset
    /** 对全部输出事件的链式哈希：同样的日志重放必须得到同样的值，不同则说明副本分歧、日志损坏或语义变了。 */
    private long chain;
    private long nextCheckpointSeq = CHECKPOINT_INTERVAL;
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
            case CMD_DEPOSIT -> deposit(account, aux, f1, f2);
            case CMD_PLACE -> place(account, aux, f1, f2, f3);
            case CMD_CANCEL -> cancel(account, f1);
            default -> emit(EVT_REJECTED, 0, 0, account, 0, REJECT_MALFORMED, 0, 0);
        }
    }

    private void deposit(long accountId, int asset, long amount, long depositId) {
        if (amount <= 0 || depositId <= 0 || (asset != ASSET_BASE && asset != ASSET_QUOTE)) {
            emit(EVT_REJECTED, 0, 0, accountId, 0, REJECT_INVALID, depositId, 0);
            return;
        }
        final ClientKey key = new ClientKey(accountId, depositId);
        final Long prior = depositOutcomes.get(key);
        if (prior != null) {
            // 重试：返回原结果，不再入账；这是 REJECTED 事件，投影无状态变化
            emit(EVT_REJECTED, 0, 0, accountId, prior, REJECT_DUPLICATE_DEPOSIT, depositId, 0);
            return;
        }
        if (supply[asset] > MAX_SUPPLY - amount) { // 先校验：超过总供应量上限直接拒绝，不改任何状态
            emit(EVT_REJECTED, 0, 0, accountId, 0, REJECT_OVERFLOW, depositId, 0);
            return;
        }
        final Account a = accounts.computeIfAbsent(accountId, k -> new Account());
        a.apply(asset, amount, 0);
        supply[asset] += amount;
        depositOutcomes.put(key, amount);
        emit(EVT_DEPOSITED, 0, 0, accountId, asset, amount, depositId, 0);
    }

    private void place(long accountId, int side, long clientOrderId, long price, long qty) {
        if ((side != SIDE_BUY && side != SIDE_SELL) || price <= 0 || qty <= 0) {
            emit(EVT_REJECTED, 0, 0, accountId, 0, REJECT_INVALID, clientOrderId, 0);
            return;
        }
        final ClientKey key = new ClientKey(accountId, clientOrderId);
        final Long prior = placeOutcomes.get(key);
        if (prior != null) {
            // 重试：返回原结果。orderId=原订单号（原本被拒则 0），price=原拒绝原因（原本被接受则 0）
            emit(EVT_REJECTED, 0, prior > 0 ? prior : 0, accountId, prior < 0 ? -prior : 0,
                    REJECT_DUPLICATE_CLIENT_ORDER, clientOrderId, 0);
            return;
        }
        final Account acct = accounts.get(accountId);
        final int freezeAsset = side == SIDE_BUY ? ASSET_QUOTE : ASSET_BASE;
        final long freezeAmount;
        try {
            freezeAmount = side == SIDE_BUY ? Math.multiplyExact(price, qty) : qty;
        } catch (ArithmeticException e) {
            placeOutcomes.put(key, (long) -REJECT_OVERFLOW);
            emit(EVT_REJECTED, 0, 0, accountId, 0, REJECT_OVERFLOW, clientOrderId, 0);
            return;
        }
        if (acct == null || acct.available[freezeAsset] < freezeAmount) {
            placeOutcomes.put(key, (long) -REJECT_INSUFFICIENT_FUNDS);
            emit(EVT_REJECTED, 0, 0, accountId, 0, REJECT_INSUFFICIENT_FUNDS, clientOrderId, 0);
            return;
        }
        // 先冻结，后撮合
        acct.apply(freezeAsset, -freezeAmount, freezeAmount);

        final Order taker = new Order(nextOrderId++, accountId, side, price, qty);
        placeOutcomes.put(key, taker.id);
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
        if (fill <= 0 || buy.price < tradePrice) {
            throw new InvariantViolation("bad trade: fill=" + fill + " tradePrice=" + tradePrice + " buyLimit=" + buy.price);
        }
        final Account b = accounts.get(buy.account);
        final Account s = accounts.get(sell.account);
        final long buyerFrozen = Math.multiplyExact(buy.price, fill);               // 按限价冻结的部分
        final long refund = Math.multiplyExact(buy.price - tradePrice, fill);       // 价格改善退还
        final long proceeds = Math.multiplyExact(tradePrice, fill);                 // 卖方所得
        b.apply(ASSET_QUOTE, refund, -buyerFrozen);
        b.apply(ASSET_BASE, fill, 0);
        s.apply(ASSET_BASE, 0, -fill);
        s.apply(ASSET_QUOTE, proceeds, 0);
    }

    private void cancel(long accountId, long orderId) {
        final Order o = ordersById.get(orderId);
        if (o == null || o.account != accountId) {
            final Long cancelledRemaining = cancelOutcomes.get(new ClientKey(accountId, orderId));
            if (cancelledRemaining != null) {
                // 重复撤单：返回原结果（REJECTED 事件，投影无状态变化，不会重复释放资金）
                emit(EVT_REJECTED, 0, orderId, accountId, cancelledRemaining, REJECT_DUPLICATE_CANCEL, orderId, 0);
                return;
            }
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
        cancelOutcomes.put(new ClientKey(accountId, orderId), o.remaining);
        final Account a = accounts.get(accountId);
        if (o.side == SIDE_BUY) {
            final long amt = Math.multiplyExact(o.price, o.remaining);
            a.apply(ASSET_QUOTE, amt, -amt);
        } else {
            a.apply(ASSET_BASE, o.remaining, -o.remaining);
        }
        emit(EVT_CANCELLED, o.side, o.id, accountId, o.price, o.remaining, 0, 0);
    }

    private static long mix(long h, long x) {
        h ^= x;
        h *= 0x9E3779B97F4A7C15L;
        h ^= h >>> 32;
        return h;
    }

    /**
     * 命令处理完之后调用：如果事件序号越过了下一个检查点边界，返回 true 并填入 {最后事件序号, 链式哈希}。
     * 边界由状态机自己确定（属于状态、随快照持久化），所有副本在同一个日志位置得到同一个检查点。
     */
    public boolean takeCheckpoint(long[] out) {
        final long last = nextEventSeq - 1;
        if (last < nextCheckpointSeq) {
            return false;
        }
        out[0] = last;
        out[1] = chain;
        nextCheckpointSeq = (last / CHECKPOINT_INTERVAL + 1) * CHECKPOINT_INTERVAL;
        return true;
    }

    public long chainHash() { return chain; }

    /** 仅测试：模拟“新代码的语义与旧日志不一致”（订单号编码变了）。 */
    public void skewNextOrderIdForTest(long delta) { nextOrderId += delta; }

    private void emit(int type, int aux, long orderId, long account, long price, long qty, long counterOrderId, long counterAccount) {
        // 参数顺序对 REJECTED/DEPOSITED 做了便捷复用：见 Protocol 注释
        final long seq = nextEventSeq++;
        long h = chain;
        h = mix(h, type | ((long) aux << 32));
        h = mix(h, seq);
        h = mix(h, now);
        h = mix(h, orderId);
        h = mix(h, account);
        h = mix(h, price);
        h = mix(h, qty);
        h = mix(h, counterOrderId);
        h = mix(h, counterAccount);
        chain = h;
        out.onEvent(type, aux, seq, now, orderId, account, price, qty, counterOrderId, counterAccount);
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

    /**
     * 全量不变量检查（O(账户+订单)，不在热路径上调用；快照前、重放结束后、测试里调用）：
     *  1. 所有余额非负；
     *  2. 每种资产 可用+冻结 的总和 == 已入账总额（守恒）；
     *  3. 每个账户的冻结额 == 它所有在簿订单所需冻结额之和（买单 price×remaining 的 quote，卖单 remaining 的 base）；
     *  4. 在簿订单剩余量 > 0。
     * 违反即抛 {@link InvariantViolation}。
     */
    public void verifyInvariants() {
        final long[] held = new long[3];
        for (Map.Entry<Long, Account> e : accounts.entrySet()) {
            final Account a = e.getValue();
            for (int asset = 1; asset <= 2; asset++) {
                if (a.available[asset] < 0 || a.frozen[asset] < 0) {
                    throw new InvariantViolation("negative balance on account " + e.getKey() + " asset " + asset);
                }
                held[asset] = Math.addExact(held[asset], Math.addExact(a.available[asset], a.frozen[asset]));
            }
        }
        for (int asset = 1; asset <= 2; asset++) {
            if (held[asset] != supply[asset]) {
                throw new InvariantViolation("conservation broken for asset " + asset + ": held=" + held[asset] + " supply=" + supply[asset]);
            }
        }
        final Map<Long, long[]> needed = new HashMap<>(); // 账户 → {base 冻结, quote 冻结}
        for (int side : new int[]{SIDE_BUY, SIDE_SELL}) {
            for (ArrayDeque<Order> level : book(side).values()) {
                for (Order o : level) {
                    if (o.remaining <= 0) {
                        throw new InvariantViolation("resting order " + o.id + " has remaining " + o.remaining);
                    }
                    final long[] n = needed.computeIfAbsent(o.account, k -> new long[2]);
                    if (o.side == SIDE_BUY) {
                        n[1] = Math.addExact(n[1], Math.multiplyExact(o.price, o.remaining));
                    } else {
                        n[0] = Math.addExact(n[0], o.remaining);
                    }
                }
            }
        }
        for (Map.Entry<Long, Account> e : accounts.entrySet()) {
            final long[] n = needed.getOrDefault(e.getKey(), new long[2]);
            final Account a = e.getValue();
            if (a.frozen[ASSET_BASE] != n[0] || a.frozen[ASSET_QUOTE] != n[1]) {
                throw new InvariantViolation("frozen funds of account " + e.getKey() + " do not match its resting orders: frozen=["
                        + a.frozen[ASSET_BASE] + "," + a.frozen[ASSET_QUOTE] + "] needed=[" + n[0] + "," + n[1] + "]");
            }
        }
    }

    // ---------- 稽核用的只读遍历（顺序确定） ----------

    @FunctionalInterface
    public interface AccountVisitor {
        void visit(long account, long availableBase, long availableQuote, long frozenBase, long frozenQuote);
    }

    @FunctionalInterface
    public interface OrderVisitor {
        void visit(long orderId, long account, int side, long price, long remaining);
    }

    public void forEachAccount(AccountVisitor v) {
        for (Map.Entry<Long, Account> e : accounts.entrySet()) {
            final Account a = e.getValue();
            v.visit(e.getKey(), a.available[ASSET_BASE], a.available[ASSET_QUOTE], a.frozen[ASSET_BASE], a.frozen[ASSET_QUOTE]);
        }
    }

    public void forEachOpenOrder(OrderVisitor v) {
        for (int side : new int[]{SIDE_BUY, SIDE_SELL}) {
            for (ArrayDeque<Order> level : book(side).values()) {
                for (Order o : level) {
                    v.visit(o.id, o.account, o.side, o.price, o.remaining);
                }
            }
        }
    }

    // ---------- 快照 ----------

    /** 序列化完整状态。遍历顺序全部有序，因此同一状态产生的字节完全一致，可直接用作状态哈希的输入。 */
    public byte[] snapshot() {
        final int orderCount = ordersById.size();
        final long size = 8L * 2 + 16 + 16 + 4 + (long) accounts.size() * (8 + 8 * 4) + 4 + (long) orderCount * (8 * 4 + 4)
                + 4 + (long) placeOutcomes.size() * 24 + 4 + (long) cancelOutcomes.size() * 24 + 4 + (long) depositOutcomes.size() * 24;
        if (size > Integer.MAX_VALUE - 1024) {
            // 数组放不下：明确失败（由服务 fail-stop），而不是整数溢出后生成一份残缺快照
            throw new IllegalStateException("state too large to snapshot in one array: " + size + " bytes");
        }
        final ByteBuffer bb = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
        bb.putLong(nextOrderId);
        bb.putLong(nextEventSeq);
        bb.putLong(supply[ASSET_BASE]).putLong(supply[ASSET_QUOTE]);
        bb.putLong(chain).putLong(nextCheckpointSeq);
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
        bb.putInt(placeOutcomes.size());
        for (Map.Entry<ClientKey, Long> e : placeOutcomes.entrySet()) {
            bb.putLong(e.getKey().account).putLong(e.getKey().clientOrderId).putLong(e.getValue());
        }
        bb.putInt(cancelOutcomes.size());
        for (Map.Entry<ClientKey, Long> e : cancelOutcomes.entrySet()) {
            bb.putLong(e.getKey().account).putLong(e.getKey().clientOrderId).putLong(e.getValue());
        }
        bb.putInt(depositOutcomes.size());
        for (Map.Entry<ClientKey, Long> e : depositOutcomes.entrySet()) {
            bb.putLong(e.getKey().account).putLong(e.getKey().clientOrderId).putLong(e.getValue());
        }
        return frame(bb.array());
    }

    /** [魔数][版本][正文][SHA-256(前面全部)]：损坏、截断、版本不符都能在加载时被发现。 */
    private static byte[] frame(byte[] body) {
        final ByteBuffer out = ByteBuffer.allocate(8 + body.length + 32).order(ByteOrder.LITTLE_ENDIAN);
        out.putInt(SNAPSHOT_MAGIC).putInt(SNAPSHOT_VERSION).put(body);
        out.put(sha256(out.array(), 0, 8 + body.length));
        return out.array();
    }

    private static byte[] unframe(byte[] data) {
        if (data == null || data.length < 8 + 32) {
            throw new SnapshotCorruptException("snapshot too short: " + (data == null ? -1 : data.length));
        }
        final ByteBuffer in = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        if (in.getInt(0) != SNAPSHOT_MAGIC) {
            throw new SnapshotCorruptException("bad snapshot magic");
        }
        if (in.getInt(4) != SNAPSHOT_VERSION) {
            throw new SnapshotCorruptException("incompatible snapshot version " + in.getInt(4) + ", expected " + SNAPSHOT_VERSION);
        }
        final byte[] expected = sha256(data, 0, data.length - 32);
        for (int i = 0; i < 32; i++) {
            if (expected[i] != data[data.length - 32 + i]) {
                throw new SnapshotCorruptException("snapshot checksum mismatch");
            }
        }
        return java.util.Arrays.copyOfRange(data, 8, data.length - 32);
    }

    private static byte[] sha256(byte[] d, int off, int len) {
        try {
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(d, off, len);
            return md.digest();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static MatchingEngine fromSnapshot(byte[] data) {
        final ByteBuffer bb = ByteBuffer.wrap(unframe(data)).order(ByteOrder.LITTLE_ENDIAN);
        final MatchingEngine e = new MatchingEngine();
        e.nextOrderId = bb.getLong();
        e.nextEventSeq = bb.getLong();
        e.supply[ASSET_BASE] = bb.getLong();
        e.supply[ASSET_QUOTE] = bb.getLong();
        e.chain = bb.getLong();
        e.nextCheckpointSeq = bb.getLong();
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
            e.placeOutcomes.put(new ClientKey(bb.getLong(), bb.getLong()), bb.getLong());
        }
        n = bb.getInt();
        for (int i = 0; i < n; i++) {
            e.cancelOutcomes.put(new ClientKey(bb.getLong(), bb.getLong()), bb.getLong());
        }
        n = bb.getInt();
        for (int i = 0; i < n; i++) {
            e.depositOutcomes.put(new ClientKey(bb.getLong(), bb.getLong()), bb.getLong());
        }
        return e;
    }

    /** 不还原整个引擎，直接从快照字节读出最后事件序号（布局：nextOrderId, nextEventSeq, ...）。 */
    public static long lastEventSeqOfSnapshot(byte[] data) {
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong(8 + 8) - 1; // 跳过 8 字节帧头和 nextOrderId
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
