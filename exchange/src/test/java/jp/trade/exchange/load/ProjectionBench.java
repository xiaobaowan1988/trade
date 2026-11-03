package jp.trade.exchange.load;

import static jp.trade.exchange.core.Protocol.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import jp.trade.exchange.TestDb;
import jp.trade.exchange.core.MatchingEngine;
import jp.trade.exchange.projection.ConnectionFactory;
import jp.trade.exchange.projection.JdbcProjector;
import jp.trade.exchange.projection.ProjectionAuditor;
import org.agrona.ExpandableArrayBuffer;

/** 投影吞吐基准：与压测同样的 1 万账户随机流，只测 事件 → MariaDB 这一段，并与引擎状态对账。 */
public final class ProjectionBench {
    public static void main(String[] args) throws Exception {
        final int accounts = Integer.getInteger("accounts", 10_000);
        final int commands = Integer.getInteger("commands", 200_000);
        final int batch = Integer.getInteger("batch", 1_000);

        final MatchingEngine engine = new MatchingEngine();
        final List<byte[]> events = new ArrayList<>();
        final ExpandableArrayBuffer cmd = new ExpandableArrayBuffer(COMMAND_LENGTH);
        final ExpandableArrayBuffer evt = new ExpandableArrayBuffer(EVENT_LENGTH);
        final Random rnd = new Random(42);
        final java.util.function.IntConsumer run = i -> engine.onCommand(cmd, 0, COMMAND_LENGTH, 1_000 + i, (t, a, s, ts, o, ac, p, q, co, ca) -> {
            encodeEvent(evt, 0, t, a, s, ts, o, ac, p, q, co, ca);
            final byte[] e = new byte[EVENT_LENGTH];
            evt.getBytes(0, e);
            events.add(e);
        });
        int step = 0;
        for (int a = 1; a <= accounts; a++) {
            encodeCommand(cmd, 0, CMD_DEPOSIT, ASSET_QUOTE, a, 10_000_000_000_000L, 2L * a, 0);
            run.accept(step++);
            encodeCommand(cmd, 0, CMD_DEPOSIT, ASSET_BASE, a, 1_000_000_000L, 2L * a + 1, 0);
            run.accept(step++);
        }
        final int cids[] = new int[accounts + 1];
        for (int i = 0; i < commands; i++) {
            final int a = 1 + rnd.nextInt(accounts);
            if (rnd.nextInt(10) == 0) {
                encodeCommand(cmd, 0, CMD_CANCEL, 0, a, 1 + rnd.nextInt(Math.max(1, i)), 0, 0);
            } else {
                encodeCommand(cmd, 0, CMD_PLACE, rnd.nextInt(2), a, ++cids[a], 100_000 + rnd.nextInt(101) - 50, 1 + rnd.nextInt(10));
            }
            run.accept(step++);
        }
        System.out.printf("生成 %d 条命令 → %d 个事件（%d 个账户）%n", step, events.size(), accounts);

        final ConnectionFactory db = TestDb.freshSchema();
        final JdbcProjector p = new JdbcProjector(db);
        final long t0 = System.nanoTime();
        for (int i = 0; i < events.size(); i += batch) {
            p.apply(new ArrayList<>(events.subList(i, Math.min(events.size(), i + batch))));
        }
        final double secs = (System.nanoTime() - t0) / 1e9;
        System.out.printf("投影 %d 个事件，批大小 %d，用时 %.1fs → %.0f 事件/s%n", events.size(), batch, secs, events.size() / secs);
        try (Connection c = db.get()) {
            final ProjectionAuditor.Result r = ProjectionAuditor.audit(c, engine);
            System.out.println("对账：" + (r.clean() ? "一致" : "不一致 " + r.diffs().subList(0, Math.min(5, r.diffs().size()))));
        }
        System.exit(0);
    }
}
