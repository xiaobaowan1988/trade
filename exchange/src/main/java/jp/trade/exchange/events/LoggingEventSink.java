package jp.trade.exchange.events;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.agrona.concurrent.UnsafeBuffer;

import static jp.trade.exchange.core.Protocol.*;

/** 无 Kafka 环境下的出口：打印并保留事件，便于演示与测试。 */
public final class LoggingEventSink implements EventSink {
    private final List<byte[]> events = new CopyOnWriteArrayList<>();
    private final boolean print;

    public LoggingEventSink(boolean print) { this.print = print; }

    @Override
    public void publish(long seq, byte[] event) {
        events.add(event);
        if (print) {
            System.out.println(describe(event));
        }
    }

    public List<byte[]> events() { return events; }

    public static String describe(byte[] e) {
        final UnsafeBuffer b = new UnsafeBuffer(e);
        final String t = switch (eventType(b, 0)) {
            case EVT_ACCEPTED -> "ACCEPTED";
            case EVT_REJECTED -> "REJECTED";
            case EVT_TRADE -> "TRADE";
            case EVT_CANCELLED -> "CANCELLED";
            case EVT_DEPOSITED -> "DEPOSITED";
            default -> "UNKNOWN";
        };
        return String.format("[evt seq=%d %s order=%d acct=%d price=%d qty=%d counterOrder=%d counterAcct=%d]",
                eventSeq(b, 0), t, eventOrderId(b, 0), eventAccount(b, 0), eventPrice(b, 0), eventQty(b, 0),
                eventCounterOrderId(b, 0), eventCounterAccount(b, 0));
    }
}
