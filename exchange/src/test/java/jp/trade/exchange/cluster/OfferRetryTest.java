package jp.trade.exchange.cluster;

import static org.junit.jupiter.api.Assertions.*;

import io.aeron.Publication;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * ADMIN_ACTION 是发布缓冲 term 轮转时的瞬时状态，“应立即重试”，不是背压；
 * 压测里把它当背压丢弃，2 万笔入金丢了 3 条确认，事件发布路径上同样会平白产生序号缺口。
 */
class OfferRetryTest {
    private final UnsafeBuffer buf = new UnsafeBuffer(new byte[72]);

    private static ExchangeService.Offerer script(AtomicInteger calls, long... results) {
        return (b, o, l) -> results[Math.min(calls.getAndIncrement(), results.length - 1)];
    }

    @Test
    void retriesAdminActionUntilItSucceeds() {
        final AtomicInteger calls = new AtomicInteger();
        final long r = ExchangeService.offerRetryingAdminAction(
                script(calls, Publication.ADMIN_ACTION, Publication.ADMIN_ACTION, 1234), buf);
        assertEquals(1234, r);
        assertEquals(3, calls.get());
    }

    @Test
    void doesNotSpinOnRealBackPressure() {
        final AtomicInteger calls = new AtomicInteger();
        final long r = ExchangeService.offerRetryingAdminAction(script(calls, Publication.BACK_PRESSURED), buf);
        assertEquals(Publication.BACK_PRESSURED, r);
        assertEquals(1, calls.get(), "背压时撮合线程不能重试/自旋");
    }

    @Test
    void givesUpAfterBoundedRetries() {
        final AtomicInteger calls = new AtomicInteger();
        final long r = ExchangeService.offerRetryingAdminAction(script(calls, Publication.ADMIN_ACTION), buf);
        assertEquals(Publication.ADMIN_ACTION, r);
        assertEquals(4, calls.get(), "首次 + 最多 3 次重试，有界");
    }

    @Test
    void notConnectedIsNotRetried() {
        final AtomicInteger calls = new AtomicInteger();
        ExchangeService.offerRetryingAdminAction(script(calls, Publication.NOT_CONNECTED), buf);
        assertEquals(1, calls.get());
    }

    // ---- 检查点的 offer：满载时曾被背压拒绝一次，该检查点静默缺失（100 万命令的正确性验证发现） ----

    @Test
    void checkpointOfferWaitsOutBackPressureInsteadOfSilentlySkipping() {
        final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger idles = new AtomicInteger();
        final long r = ExchangeService.offerBounded(script(calls, Publication.BACK_PRESSURED, Publication.BACK_PRESSURED, 99),
                buf, 20, 1_000_000_000L, idles::incrementAndGet);
        assertEquals(99, r);
        assertEquals(3, calls.get());
        assertEquals(2, idles.get(), "背压期间让出 CPU 等待，而不是空转或放弃");
    }

    @Test
    void checkpointOfferGivesUpAfterTheBoundAndReportsFailure() {
        final AtomicInteger calls = new AtomicInteger();
        final long r = ExchangeService.offerBounded(script(calls, Publication.BACK_PRESSURED), buf, 20, 20_000_000L, () -> { });
        assertEquals(Publication.BACK_PRESSURED, r, "有界：到点放弃，由调用方计入失败，而不是无限等待");
        assertTrue(calls.get() > 1);
    }

    @Test
    void checkpointOfferDoesNotWaitOnClosedAndDoesNotWaitAtAllOnFollowers() {
        final AtomicInteger calls = new AtomicInteger();
        assertEquals(Publication.CLOSED, ExchangeService.offerBounded(script(calls, Publication.CLOSED), buf, 20, 5_000_000_000L, () -> { }));
        assertEquals(1, calls.get());
        final AtomicInteger c2 = new AtomicInteger();
        ExchangeService.offerBounded(script(c2, Publication.BACK_PRESSURED), buf, 20, 0, () -> { }); // follower：maxWait=0
        assertEquals(1, c2.get(), "follower 不等待");
    }
}
