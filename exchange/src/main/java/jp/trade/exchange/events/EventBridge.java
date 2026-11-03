package jp.trade.exchange.events;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import static jp.trade.exchange.core.Protocol.EVENT_LENGTH;
import static jp.trade.exchange.core.Protocol.eventSeq;

/**
 * 事件发布线程：订阅撮合服务写入的 Aeron IPC 流（无锁环形缓冲），在独立线程里把事件交给 EventSink。
 *
 * “可以慢，不能丢”：按批取出事件，对整批 publish + flush，成功后才取下一批；任何失败（含 flush 抛出的
 * 下游确认失败）都对同一批退避重试，线程绝不因异常退出。重试期间缓冲逐渐填满，反压到撮合线程（变慢），
 * 而不是丢。重试可能产生重复，消费端按事件序号去重。
 */
public final class EventBridge implements AutoCloseable {
    public static final String CHANNEL = "aeron:ipc";
    public static final int STREAM_ID = 3001;
    private static final int MAX_BATCH = 256;

    private final Aeron aeron;
    private final Subscription subscription;
    private final EventSink sink;
    private final EventDeduper deduper = new EventDeduper();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong sinkFailures = new AtomicLong();
    private volatile Throwable lastError;
    private final Thread thread;

    public EventBridge(String aeronDir, EventSink sink) {
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        this.subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
        this.sink = sink;
        this.thread = new Thread(this::run, "event-bridge");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    private void run() {
        final IdleStrategy idle = new BackoffIdleStrategy(100, 10, 1_000, 1_000_000);
        final List<byte[]> batch = new ArrayList<>(MAX_BATCH);
        final FragmentHandler handler = (buffer, offset, length, header) -> {
            if (length != EVENT_LENGTH) {
                return;
            }
            if (deduper.check(eventSeq(buffer, offset)) == EventDeduper.Verdict.DUPLICATE) {
                return;
            }
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            batch.add(copy);
        };
        while (running.get()) {
            batch.clear();
            final int n;
            try {
                n = subscription.poll(handler, MAX_BATCH);
            } catch (RuntimeException e) {
                recordFailure(e);
                sleep(100);
                continue;
            }
            if (batch.isEmpty()) {
                idle.idle(n);
                continue;
            }
            deliverReliably(batch);
        }
    }

    /** 对整批退避重试直到成功（或被关闭）。 */
    private void deliverReliably(List<byte[]> batch) {
        long backoffMs = 50;
        while (running.get()) {
            try {
                for (byte[] e : batch) {
                    sink.publish(eventSeq(new org.agrona.concurrent.UnsafeBuffer(e), 0), e);
                }
                sink.flush();
                return;
            } catch (Throwable t) { // 包括 Error 之外的一切：线程不能因异常而静默退出
                recordFailure(t);
                sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 5_000);
            }
        }
    }

    private void recordFailure(Throwable t) {
        sinkFailures.incrementAndGet();
        lastError = t;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public EventDeduper deduper() { return deduper; }

    /** 下游发送/确认失败（并已重试）的累计次数。 */
    public long sinkFailures() { return sinkFailures.get(); }

    public Throwable lastError() { return lastError; }

    @Override
    public void close() throws Exception {
        running.set(false);
        thread.join(5_000);
        subscription.close();
        aeron.close();
        sink.close();
    }
}
