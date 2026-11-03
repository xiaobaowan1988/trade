package jp.trade.exchange;

import static jp.trade.exchange.core.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import jp.trade.exchange.cluster.ExchangeClient;
import jp.trade.exchange.cluster.ExchangeNode;
import jp.trade.exchange.events.LoggingEventSink;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/** “服务可以挂，但不能出错”：状态机出现异常时必须停下，而不是吞掉命令继续跑。 */
class FailStopTest {
    @Test
    void anExceptionInTheStateMachineStopsTheServiceInsteadOfSkippingTheCommand() throws Exception {
        final File dir = Files.createTempDirectory("fs").toFile();
        final List<Throwable> fatals = new CopyOnWriteArrayList<>();
        final LoggingEventSink sink = new LoggingEventSink(false);
        final java.util.function.Consumer<jp.trade.exchange.cluster.ExchangeService> cfg =
                svc -> svc.fatalHandler(fatals::add).injectFailureAtCommand(3);

        final List<Integer> answered = new ArrayList<>();
        try (ExchangeNode node = ExchangeNode.start(0, 1, dir, true, sink, c -> {}, cfg);
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            for (int i = 1; i <= 6; i++) {
                try {
                    c.deposit(i, ASSET_QUOTE, 1_000, 100 + i); // 显式 depositId，重启后用来检查“命令 3 没有丢”
                    c.await(1, 1_500);
                    answered.add(i);
                } catch (IllegalStateException noResponse) {
                    // 服务停机后：共识模块也随之退出，发送失败（已关闭）或超时，都是“没有响应”
                }
            }
            Thread.sleep(300);
            assertEquals(List.of(1, 2), answered, "第 3 条命令失败之后，服务不能再处理任何命令");
            assertEquals(1, fatals.size(), "致命错误处理器只触发一次");
            assertTrue(fatals.get(0).getCause().getMessage().contains("injected"));
            assertTrue(node.service().halted());
            assertEquals(2, sink.events().size(), "没有为失败的命令及之后的命令发布任何事件");
        }

        // 停机不丢：命令 3 还在日志里。去掉故障后重启，重放应当正确应用它——重试同一笔入金得到“重复”
        final LoggingEventSink sink2 = new LoggingEventSink(false);
        try (ExchangeNode node = ExchangeNode.start(dir, false, sink2);
             ExchangeClient c = new ExchangeClient(node.aeronDirectoryName())) {
            c.deposit(3, ASSET_QUOTE, 1_000, 103);
            final byte[] r = c.await(1, 15_000).get(0);
            final UnsafeBuffer b = new UnsafeBuffer(r);
            assertEquals(EVT_REJECTED, eventType(b, 0));
            assertEquals(REJECT_DUPLICATE_DEPOSIT, eventQty(b, 0), "命令 3 在重放时已被正确应用，没有丢");
            assertEquals(1_000, eventPrice(b, 0));
            c.deposit(7, ASSET_QUOTE, 1_000, 107);
            assertEquals(EVT_DEPOSITED, eventType(new UnsafeBuffer(c.await(1, 15_000).get(0)), 0), "命令 4、5、6 也在日志里，之后继续正常服务");
        }
    }
}
