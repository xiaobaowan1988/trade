package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import jp.trade.exchange.core.InvariantViolation;
import jp.trade.exchange.core.MatchingEngine;
import jp.trade.exchange.core.SnapshotCorruptException;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

class MatchingEngineTest {
    record Evt(int type, int aux, long seq, long ts, long orderId, long account, long price, long qty, long cOrder, long cAcct) {}

    private final ExpandableArrayBuffer buf = new ExpandableArrayBuffer(COMMAND_LENGTH);

    private List<Evt> run(MatchingEngine e, int type, int aux, long acct, long f1, long f2, long f3) {
        final List<Evt> out = new ArrayList<>();
        encodeCommand(buf, 0, type, aux, acct, f1, f2, f3);
        e.onCommand(buf, 0, COMMAND_LENGTH, 1_000, (t, a, s, ts, o, ac, p, q, co, ca) -> out.add(new Evt(t, a, s, ts, o, ac, p, q, co, ca)));
        return out;
    }

    private long nextDepositId = 1;
    private List<Evt> deposit(MatchingEngine e, long acct, int asset, long amt) { return run(e, CMD_DEPOSIT, asset, acct, amt, nextDepositId++, 0); }
    private List<Evt> depositWithId(MatchingEngine e, long acct, int asset, long amt, long id) { return run(e, CMD_DEPOSIT, asset, acct, amt, id, 0); }
    private List<Evt> place(MatchingEngine e, long acct, int side, long cid, long price, long qty) { return run(e, CMD_PLACE, side, acct, cid, price, qty); }
    private List<Evt> cancel(MatchingEngine e, long acct, long orderId) { return run(e, CMD_CANCEL, 0, acct, orderId, 0, 0); }

    @Test
    void pricePriorityThenTimePriority() {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_BASE, 100);
        deposit(e, 2, ASSET_BASE, 100);
        deposit(e, 3, ASSET_QUOTE, 1_000_000);
        place(e, 1, SIDE_SELL, 1, 5000, 5);  // order 1：先到
        place(e, 2, SIDE_SELL, 1, 5000, 5);  // order 2：同价后到
        place(e, 1, SIDE_SELL, 2, 4900, 5);  // order 3：更优价
        final List<Evt> ev = place(e, 3, SIDE_BUY, 1, 5000, 12);
        final List<Evt> trades = ev.stream().filter(x -> x.type == EVT_TRADE).toList();
        assertEquals(3, trades.size());
        assertEquals(3, trades.get(0).cOrder);  // 最优价先成交
        assertEquals(4900, trades.get(0).price);
        assertEquals(1, trades.get(1).cOrder);  // 同价：先到先成交
        assertEquals(2, trades.get(2).cOrder);
        assertEquals(2, trades.get(2).qty);     // 12 = 5 + 5 + 2
        assertEquals(1, e.restingOrderCount()); // order 2 剩 3
    }

    @Test
    void tradesAtMakerPriceAndRefundsPriceImprovement() {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_BASE, 10);
        deposit(e, 2, ASSET_QUOTE, 100_000);
        place(e, 1, SIDE_SELL, 1, 5000, 10);
        place(e, 2, SIDE_BUY, 1, 5100, 10);
        assertEquals(100_000 - 50_000, e.available(2, ASSET_QUOTE)); // 以 5000 成交，多冻结的 1000 已退回
        assertEquals(0, e.frozen(2, ASSET_QUOTE));
        assertEquals(10, e.available(2, ASSET_BASE));
        assertEquals(50_000, e.available(1, ASSET_QUOTE));
        assertEquals(0, e.frozen(1, ASSET_BASE));
    }

    @Test
    void rejectsInsufficientFundsAndFreezesBeforeMatching() {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_QUOTE, 1_000);
        final List<Evt> ev = place(e, 1, SIDE_BUY, 1, 5000, 1); // 需要 5000
        assertEquals(EVT_REJECTED, ev.get(0).type);
        assertEquals(REJECT_INSUFFICIENT_FUNDS, ev.get(0).qty);
        // 挂单后可用余额被冻结，不能重复使用
        deposit(e, 2, ASSET_QUOTE, 5_000);
        place(e, 2, SIDE_BUY, 1, 5000, 1);
        assertEquals(0, e.available(2, ASSET_QUOTE));
        assertEquals(EVT_REJECTED, place(e, 2, SIDE_BUY, 2, 1, 1).get(0).type);
    }

    @Test
    void duplicateClientOrderIdIsIdempotent() {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_QUOTE, 100_000);
        assertEquals(EVT_ACCEPTED, place(e, 1, SIDE_BUY, 7, 100, 1).get(0).type);
        final List<Evt> dup = place(e, 1, SIDE_BUY, 7, 100, 1);
        assertEquals(EVT_REJECTED, dup.get(0).type);
        assertEquals(REJECT_DUPLICATE_CLIENT_ORDER, dup.get(0).qty);
        assertEquals(1, e.restingOrderCount());
        assertEquals(100, e.frozen(1, ASSET_QUOTE));
    }

    @Test
    void cancelReleasesFundsAndChecksOwner() {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_QUOTE, 10_000);
        place(e, 1, SIDE_BUY, 1, 100, 10); // order 1
        assertEquals(EVT_REJECTED, cancel(e, 2, 1).get(0).type); // 非所有者
        assertEquals(EVT_CANCELLED, cancel(e, 1, 1).get(0).type);
        assertEquals(10_000, e.available(1, ASSET_QUOTE));
        assertEquals(0, e.frozen(1, ASSET_QUOTE));
        assertEquals(EVT_REJECTED, cancel(e, 1, 1).get(0).type); // 已撤销
    }

    @Test
    void overflowIsRejectedNotWrapped() {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_QUOTE, 1_000);
        final List<Evt> ev = place(e, 1, SIDE_BUY, 1, Long.MAX_VALUE, 2);
        assertEquals(REJECT_OVERFLOW, ev.get(0).qty);
    }

    /** 随机命令流：资产守恒 + 两个引擎重放结果完全一致（确定性）+ 快照恢复后继续一致。 */
    @Test
    void conservationDeterminismAndSnapshotRoundTrip() {
        final long seed = 42;
        final MatchingEngine a = new MatchingEngine();
        final MatchingEngine b = new MatchingEngine();
        MatchingEngine restored = null;
        final List<Evt> evA = new ArrayList<>();
        final List<Evt> evB = new ArrayList<>();
        final List<Evt> evR = new ArrayList<>();
        long depositedBase = 0, depositedQuote = 0;

        final Random rnd = new Random(seed);
        final int steps = 20_000;
        for (int i = 0; i < steps; i++) {
            final int kind = rnd.nextInt(10);
            final long acct = 1 + rnd.nextInt(8);
            final int type;
            final int aux;
            final long f1, f2, f3;
            if (kind == 0) {
                type = CMD_DEPOSIT;
                aux = 1 + rnd.nextInt(2);
                f1 = 1 + rnd.nextInt(50_000);
                f2 = i + 1; // depositId：每笔入金唯一
                f3 = 0;
                if (aux == ASSET_BASE) depositedBase += f1; else depositedQuote += f1;
            } else if (kind == 1) {
                type = CMD_CANCEL;
                aux = 0;
                f1 = 1 + rnd.nextInt(Math.max(1, i / 3));
                f2 = 0;
                f3 = 0;
            } else {
                type = CMD_PLACE;
                aux = rnd.nextInt(2);
                f1 = i;
                f2 = 95 + rnd.nextInt(11);
                f3 = 1 + rnd.nextInt(20);
            }
            encodeCommand(buf, 0, type, aux, acct, f1, f2, f3);
            final boolean isDeposit = type == CMD_DEPOSIT;
            // 同时记录“被拒绝的存款”不应计入守恒：存款额都小于溢出界，必然成功
            a.onCommand(buf, 0, COMMAND_LENGTH, i, (t, x, s, ts, o, ac, p, q, co, ca) -> evA.add(new Evt(t, x, s, ts, o, ac, p, q, co, ca)));
            b.onCommand(buf, 0, COMMAND_LENGTH, i, (t, x, s, ts, o, ac, p, q, co, ca) -> evB.add(new Evt(t, x, s, ts, o, ac, p, q, co, ca)));
            if (restored != null) {
                restored.onCommand(buf, 0, COMMAND_LENGTH, i, (t, x, s, ts, o, ac, p, q, co, ca) -> evR.add(new Evt(t, x, s, ts, o, ac, p, q, co, ca)));
            }
            if (i == steps / 2) {
                restored = MatchingEngine.fromSnapshot(a.snapshot()); // 中途做快照并恢复
                assertEquals(a.stateHash(), restored.stateHash());
            }
            if (i % 500 == 0) {
                a.verifyInvariants(); // 余额非负、守恒、冻结额与在簿订单一致
            }
            assertEquals(depositedBase, a.totalHeld(ASSET_BASE), "base 守恒 @" + i);
            assertEquals(depositedQuote, a.totalHeld(ASSET_QUOTE), "quote 守恒 @" + i);
        }
        assertEquals(evA, evB);
        assertEquals(a.stateHash(), b.stateHash());
        assertEquals(a.stateHash(), restored.stateHash());      // 快照恢复后行为与原引擎一致
        assertEquals(evA.subList(evA.size() - evR.size(), evA.size()), evR);
        assertTrue(evA.stream().anyMatch(x -> x.type == EVT_TRADE), "随机流应产生成交");
        assertTrue(evA.stream().anyMatch(x -> x.type == EVT_CANCELLED), "随机流应产生撤单");
    }

    // ---------- “可以慢，不能丢”：回报丢了，重试必须取回原结果 ----------

    @Test
    void retryOfAcceptedOrderReturnsTheOriginalOrderId() {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_QUOTE, 100_000);
        final long orderId = place(e, 1, SIDE_BUY, 7, 100, 3).get(0).orderId;   // 首次：回报“丢了”
        final List<Evt> retry = place(e, 1, SIDE_BUY, 7, 100, 3);
        assertEquals(1, retry.size());
        assertEquals(EVT_REJECTED, retry.get(0).type);
        assertEquals(REJECT_DUPLICATE_CLIENT_ORDER, retry.get(0).qty);
        assertEquals(orderId, retry.get(0).orderId, "返回原订单号");
        assertEquals(0, retry.get(0).price, "原本是被接受的");
        assertEquals(1, e.restingOrderCount(), "没有重复下单");
        assertEquals(300, e.frozen(1, ASSET_QUOTE), "没有重复冻结");
    }

    @Test
    void retryOfRejectedOrderReturnsTheOriginalReasonEvenIfFundsArriveLater() {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_QUOTE, 10);
        assertEquals(REJECT_INSUFFICIENT_FUNDS, place(e, 1, SIDE_BUY, 7, 100, 3).get(0).qty);
        deposit(e, 1, ASSET_QUOTE, 1_000_000);            // 之后资金到账
        final List<Evt> retry = place(e, 1, SIDE_BUY, 7, 100, 3);
        assertEquals(REJECT_DUPLICATE_CLIENT_ORDER, retry.get(0).qty);
        assertEquals(0, retry.get(0).orderId, "原本就没有订单");
        assertEquals(REJECT_INSUFFICIENT_FUNDS, retry.get(0).price, "返回原拒绝原因，而不是重新校验");
        assertEquals(0, e.restingOrderCount());
    }

    @Test
    void retryOfCancelReturnsTheOriginalResultAndDoesNotReleaseFundsTwice() {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_QUOTE, 10_000);
        place(e, 1, SIDE_BUY, 1, 100, 10); // order 1
        assertEquals(EVT_CANCELLED, cancel(e, 1, 1).get(0).type);
        final long availableAfterFirst = e.available(1, ASSET_QUOTE);

        final List<Evt> retry = cancel(e, 1, 1);
        assertEquals(1, retry.size());
        assertEquals(EVT_REJECTED, retry.get(0).type, "重复撤单不能再产生 CANCELLED 事件，否则投影会重复释放资金");
        assertEquals(REJECT_DUPLICATE_CANCEL, retry.get(0).qty);
        assertEquals(1, retry.get(0).orderId);
        assertEquals(10, retry.get(0).price, "返回原撤销的剩余数量");
        assertEquals(availableAfterFirst, e.available(1, ASSET_QUOTE));
        // 别的账户撤不了它，也拿不到这个结果
        assertEquals(REJECT_UNKNOWN_ORDER, cancel(e, 2, 1).get(0).qty);
    }

    @Test
    void outcomesSurviveSnapshotRestore() {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_QUOTE, 10_000);
        place(e, 1, SIDE_BUY, 1, 100, 10);
        place(e, 1, SIDE_BUY, 2, 100, 100_000); // 资金不足，被拒
        cancel(e, 1, 1);
        final MatchingEngine r = MatchingEngine.fromSnapshot(e.snapshot());
        assertEquals(e.stateHash(), r.stateHash());
        assertEquals(REJECT_DUPLICATE_CLIENT_ORDER, place(r, 1, SIDE_BUY, 1, 100, 10).get(0).qty);
        assertEquals(REJECT_INSUFFICIENT_FUNDS, place(r, 1, SIDE_BUY, 2, 100, 100_000).get(0).price);
        assertEquals(REJECT_DUPLICATE_CANCEL, cancel(r, 1, 1).get(0).qty);
    }

    @Test
    void retryOfDepositIsIdempotentSoMoneyIsNeverCreditedTwice() {
        final MatchingEngine e = new MatchingEngine();
        assertEquals(EVT_DEPOSITED, depositWithId(e, 1, ASSET_QUOTE, 5_000, 77).get(0).type);   // 首次：回报“丢了”
        final List<Evt> retry = depositWithId(e, 1, ASSET_QUOTE, 5_000, 77);                    // 超时重试
        assertEquals(1, retry.size());
        assertEquals(EVT_REJECTED, retry.get(0).type, "重试不能再产生 DEPOSITED 事件，否则投影会再入账一次");
        assertEquals(REJECT_DUPLICATE_DEPOSIT, retry.get(0).qty);
        assertEquals(5_000, retry.get(0).price, "返回原入金金额");
        assertEquals(77, retry.get(0).cOrder);
        assertEquals(5_000, e.available(1, ASSET_QUOTE), "只入账一次");
        assertEquals(5_000, e.totalHeld(ASSET_QUOTE));

        // 同一个 depositId 在不同账户互不影响；不同 depositId 是新的入金
        assertEquals(EVT_DEPOSITED, depositWithId(e, 2, ASSET_QUOTE, 100, 77).get(0).type);
        assertEquals(EVT_DEPOSITED, depositWithId(e, 1, ASSET_QUOTE, 5_000, 78).get(0).type);
        assertEquals(10_000, e.available(1, ASSET_QUOTE));
    }

    @Test
    void depositWithoutAnIdempotencyKeyIsRejected() {
        final MatchingEngine e = new MatchingEngine();
        assertEquals(REJECT_INVALID, depositWithId(e, 1, ASSET_QUOTE, 5_000, 0).get(0).qty);
        assertEquals(0, e.totalHeld(ASSET_QUOTE), "没有幂等键的入金不被接受");
    }

    // ---------- “服务可以挂，但不能出错”：数据安全 ----------

    @Test
    void depositBeyondTheSupplyCapIsRejectedBeforeAnyStateChange() {
        final MatchingEngine e = new MatchingEngine();
        assertEquals(EVT_DEPOSITED, deposit(e, 1, ASSET_QUOTE, MatchingEngine.MAX_SUPPLY - 10).get(0).type);
        final List<Evt> over = deposit(e, 2, ASSET_QUOTE, 11);           // 总额会超过上限
        assertEquals(REJECT_OVERFLOW, over.get(0).qty);
        assertEquals(0, e.available(2, ASSET_QUOTE), "被拒绝的入金不能留下任何状态（包括凭空创建的账户）");
        assertEquals(EVT_DEPOSITED, deposit(e, 2, ASSET_QUOTE, 10).get(0).type, "恰好到上限仍可入账");
        e.verifyInvariants();
    }

    @Test
    void extremeValuesNeverOverflowAndConservationHolds() {
        final MatchingEngine e = new MatchingEngine();
        final long half = MatchingEngine.MAX_SUPPLY / 2;
        deposit(e, 1, ASSET_QUOTE, half);                 // 买方：约 1.15e18 JPY
        deposit(e, 2, ASSET_BASE, 1_000_000_000L);        // 卖方：10 亿 lot
        place(e, 2, SIDE_SELL, 1, half / 1_000_000_000L, 1_000_000_000L);   // 价格 × 数量 ≈ 全部余额
        final List<Evt> buy = place(e, 1, SIDE_BUY, 1, half / 1_000_000_000L, 1_000_000_000L);
        assertTrue(buy.stream().anyMatch(x -> x.type == EVT_TRADE));
        e.verifyInvariants();
        // 乘积溢出的订单在改状态之前就被拒绝
        deposit(e, 3, ASSET_QUOTE, 1_000);
        assertEquals(REJECT_OVERFLOW, place(e, 3, SIDE_BUY, 9, Long.MAX_VALUE / 2, 5).get(0).qty);
        e.verifyInvariants();
    }

    private static void corrupt(MatchingEngine e, long account, int asset, long newFrozen) throws Exception {
        final java.lang.reflect.Field f = MatchingEngine.class.getDeclaredField("accounts");
        f.setAccessible(true);
        final Object acct = ((java.util.Map<?, ?>) f.get(e)).get(account);
        final java.lang.reflect.Field fr = acct.getClass().getDeclaredField("frozen");
        fr.setAccessible(true);
        ((long[]) fr.get(acct))[asset] = newFrozen;
    }

    @Test
    void aCorruptedBalanceIsDetectedInsteadOfBeingServed() throws Exception {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_QUOTE, 1_000);
        place(e, 1, SIDE_BUY, 1, 100, 5);          // 冻结 500
        e.verifyInvariants();
        corrupt(e, 1, ASSET_QUOTE, 100);           // 模拟内存/逻辑错误：冻结额少于订单所需
        assertThrows(InvariantViolation.class, e::verifyInvariants, "全量检查发现冻结额与在簿订单不一致");
        // 即使没有全量检查，下一次触及这个账户的命令也会因为余额变负而抛出，而不是静默算出错误结果
        assertThrows(InvariantViolation.class, () -> cancel(e, 1, 1));
    }

    @Test
    void conservationBreakIsDetected() throws Exception {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_QUOTE, 1_000);
        corrupt(e, 1, ASSET_QUOTE, 7);             // 凭空多出 7 的冻结额
        assertThrows(InvariantViolation.class, e::verifyInvariants);
    }

    @Test
    void corruptedOrTruncatedOrIncompatibleSnapshotsAreRefused() {
        final MatchingEngine e = new MatchingEngine();
        deposit(e, 1, ASSET_QUOTE, 10_000);
        place(e, 1, SIDE_BUY, 1, 100, 10);
        final byte[] snap = e.snapshot();
        assertEquals(e.stateHash(), MatchingEngine.fromSnapshot(snap).stateHash(), "完好的快照可以加载");

        for (int pos : new int[]{9, snap.length / 2, snap.length - 40}) {   // 正文里任意一个字节翻转
            final byte[] bad = snap.clone();
            bad[pos] ^= 0x01;
            assertThrows(SnapshotCorruptException.class, () -> MatchingEngine.fromSnapshot(bad), "翻转字节 " + pos);
        }
        assertThrows(SnapshotCorruptException.class, () -> MatchingEngine.fromSnapshot(java.util.Arrays.copyOf(snap, snap.length - 1)), "截断");
        assertThrows(SnapshotCorruptException.class, () -> MatchingEngine.fromSnapshot(new byte[10]), "太短");
        final byte[] wrongVersion = snap.clone();
        wrongVersion[4] ^= 0x7F;                                              // 版本号不符（连校验和也对不上，仍应被明确拒绝）
        assertThrows(SnapshotCorruptException.class, () -> MatchingEngine.fromSnapshot(wrongVersion));
        final byte[] wrongMagic = snap.clone();
        wrongMagic[0] ^= 0x01;
        assertThrows(SnapshotCorruptException.class, () -> MatchingEngine.fromSnapshot(wrongMagic));
    }
}
