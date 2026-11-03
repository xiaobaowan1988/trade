package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import jp.trade.exchange.core.MatchingEngine;
import org.agrona.ExpandableArrayBuffer;

/** 生成随机命令流并跑过撮合引擎，得到事件字节流与最终引擎状态。 */
final class EngineStream {
    final List<byte[]> events = new ArrayList<>();
    final MatchingEngine engine = new MatchingEngine();

    static EngineStream generate(long seed, int steps) {
        final EngineStream out = new EngineStream();
        final ExpandableArrayBuffer cmd = new ExpandableArrayBuffer(COMMAND_LENGTH);
        final ExpandableArrayBuffer evt = new ExpandableArrayBuffer(EVENT_LENGTH);
        final Random rnd = new Random(seed);
        for (int i = 0; i < steps; i++) {
            final int kind = rnd.nextInt(10);
            final long acct = 1 + rnd.nextInt(8);
            if (kind == 0 || i < 16) {
                encodeCommand(cmd, 0, CMD_DEPOSIT, 1 + rnd.nextInt(2), 1 + (i % 8), 1 + rnd.nextInt(50_000), 0, 0);
            } else if (kind == 1) {
                encodeCommand(cmd, 0, CMD_CANCEL, 0, acct, 1 + rnd.nextInt(Math.max(1, i / 3)), 0, 0);
            } else {
                encodeCommand(cmd, 0, CMD_PLACE, rnd.nextInt(2), acct, i, 95 + rnd.nextInt(11), 1 + rnd.nextInt(20));
            }
            out.engine.onCommand(cmd, 0, COMMAND_LENGTH, 1_000 + i, (t, a, s, ts, o, ac, p, q, co, ca) -> {
                encodeEvent(evt, 0, t, a, s, ts, o, ac, p, q, co, ca);
                final byte[] e = new byte[EVENT_LENGTH];
                evt.getBytes(0, e);
                out.events.add(e);
            });
        }
        return out;
    }
}
