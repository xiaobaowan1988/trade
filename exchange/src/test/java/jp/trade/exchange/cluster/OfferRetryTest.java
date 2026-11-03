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
}
