package jp.trade.exchange.events;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import static jp.trade.exchange.core.Protocol.EVENT_LENGTH;
import static jp.trade.exchange.core.Protocol.eventSeq;

/**
 * 事件发布线程：订阅撮合服务写入的 Aeron IPC 流（无锁环形缓冲），在独立线程里去重后交给 EventSink。
 * 撮合线程只做一次 IPC offer，Kafka 的网络 I/O 与阻塞完全发生在这里。
 */
public final class EventBridge implements AutoCloseable {
    public static final String CHANNEL = "aeron:ipc";
    public static final int STREAM_ID = 3001;

    private final Aeron aeron;
    private final Subscription subscription;
    private final EventSink sink;
    private final EventDeduper deduper = new EventDeduper();
    private final AtomicBoolean running = new AtomicBoolean(true);
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
        final FragmentHandler handler = (buffer, offset, length, header) -> {
            if (length != EVENT_LENGTH) {
                return;
            }
            final long seq = eventSeq(buffer, offset);
            if (deduper.check(seq) == EventDeduper.Verdict.DUPLICATE) {
                return;
            }
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            sink.publish(seq, copy);
        };
        while (running.get()) {
            idle.idle(subscription.poll(handler, 64));
        }
    }

    public EventDeduper deduper() { return deduper; }

    @Override
    public void close() throws Exception {
        running.set(false);
        thread.join(2_000);
        subscription.close();
        aeron.close();
        sink.close();
    }
}
