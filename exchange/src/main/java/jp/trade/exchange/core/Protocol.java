package jp.trade.exchange.core;

import org.agrona.MutableDirectBuffer;
import org.agrona.DirectBuffer;

/**
 * 定长二进制协议（骨架阶段手写，字段布局稳定后可无缝替换为 SBE）。
 * 全部小端；金额/数量/价格均为 long 定点整数，无浮点。
 *
 * 命令 (48B): int type | int aux | long account | long f1 | long f2 | long f3
 *   DEPOSIT: aux=asset, f1=amount
 *   PLACE  : aux=side,  f1=clientOrderId, f2=price, f3=qty
 *   CANCEL : f1=orderId
 *
 * 事件 (72B): int type | int aux | long seq | long ts | long orderId | long account
 *             | long price | long qty | long counterOrderId | long counterAccount
 */
public final class Protocol {
    private Protocol() {}

    public static final int MARKET_ID = 1; // BTC/JPY，骨架仅单市场
    public static final String MARKET_NAME = "BTC-JPY";

    public static final int ASSET_BASE = 1;  // BTC，数量单位为 lot
    public static final int ASSET_QUOTE = 2; // JPY，价格为“每 lot 的 JPY”，成交额 = price * qty（无舍入）

    public static final int SIDE_BUY = 0;
    public static final int SIDE_SELL = 1;

    // 命令类型
    public static final int CMD_DEPOSIT = 1;
    public static final int CMD_PLACE = 2;
    public static final int CMD_CANCEL = 3;
    public static final int COMMAND_LENGTH = 48;

    // 事件类型
    public static final int EVT_ACCEPTED = 1;   // orderId, account, price, qty, counterOrderId=clientOrderId, aux=side
    public static final int EVT_REJECTED = 2;   // account, counterOrderId=clientOrderId(或 orderId), qty=reason
    public static final int EVT_TRADE = 3;      // orderId=taker, counterOrderId=maker, account=taker, counterAccount=maker, aux=takerSide
    public static final int EVT_CANCELLED = 4;  // orderId, account, qty=剩余数量
    public static final int EVT_DEPOSITED = 5;  // account, price=asset, qty=amount
    public static final int EVENT_LENGTH = 72;

    // 拒绝原因
    public static final int REJECT_INSUFFICIENT_FUNDS = 1;
    public static final int REJECT_DUPLICATE_CLIENT_ORDER = 2;
    public static final int REJECT_INVALID = 3;
    public static final int REJECT_UNKNOWN_ORDER = 4;
    public static final int REJECT_MALFORMED = 5;
    public static final int REJECT_OVERFLOW = 6;

    public static int encodeCommand(MutableDirectBuffer b, int o, int type, int aux, long account, long f1, long f2, long f3) {
        b.putInt(o, type);
        b.putInt(o + 4, aux);
        b.putLong(o + 8, account);
        b.putLong(o + 16, f1);
        b.putLong(o + 24, f2);
        b.putLong(o + 32, f3);
        b.putLong(o + 40, 0L); // 保留
        return COMMAND_LENGTH;
    }

    public static int encodeEvent(MutableDirectBuffer b, int o, int type, int aux, long seq, long ts, long orderId,
                                  long account, long price, long qty, long counterOrderId, long counterAccount) {
        b.putInt(o, type);
        b.putInt(o + 4, aux);
        b.putLong(o + 8, seq);
        b.putLong(o + 16, ts);
        b.putLong(o + 24, orderId);
        b.putLong(o + 32, account);
        b.putLong(o + 40, price);
        b.putLong(o + 48, qty);
        b.putLong(o + 56, counterOrderId);
        b.putLong(o + 64, counterAccount);
        return EVENT_LENGTH;
    }

    public static int eventType(DirectBuffer b, int o) { return b.getInt(o); }
    public static int eventAux(DirectBuffer b, int o) { return b.getInt(o + 4); }
    public static long eventSeq(DirectBuffer b, int o) { return b.getLong(o + 8); }
    public static long eventTimestamp(DirectBuffer b, int o) { return b.getLong(o + 16); }
    public static long eventOrderId(DirectBuffer b, int o) { return b.getLong(o + 24); }
    public static long eventAccount(DirectBuffer b, int o) { return b.getLong(o + 32); }
    public static long eventPrice(DirectBuffer b, int o) { return b.getLong(o + 40); }
    public static long eventQty(DirectBuffer b, int o) { return b.getLong(o + 48); }
    public static long eventCounterOrderId(DirectBuffer b, int o) { return b.getLong(o + 56); }
    public static long eventCounterAccount(DirectBuffer b, int o) { return b.getLong(o + 64); }
}
