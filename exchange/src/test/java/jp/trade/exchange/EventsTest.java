package jp.trade.exchange;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import jp.trade.exchange.events.EventDeduper;
import jp.trade.exchange.events.EventDeduper.Verdict;
import jp.trade.exchange.events.KafkaEventSink;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

class EventsTest {
    @Test
    void deduperDetectsDuplicatesAndGaps() {
        final EventDeduper d = new EventDeduper();
        assertEquals(Verdict.ACCEPT, d.check(10));
        assertEquals(Verdict.ACCEPT, d.check(11));
        assertEquals(Verdict.DUPLICATE, d.check(11));
        assertEquals(Verdict.DUPLICATE, d.check(5));
        assertEquals(Verdict.GAP, d.check(14));
        assertEquals(1, d.gaps());
        assertEquals(2, d.duplicates());
        assertEquals(14, d.lastSeq());
    }

    @Test
    void kafkaSinkKeepsSameKeyForOnePartition() {
        final MockProducer<byte[], byte[]> mock = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        final KafkaEventSink sink = new KafkaEventSink(mock, "t");
        sink.publish(1, new byte[72]);
        sink.publish(2, new byte[72]);
        final List<?> sent = mock.history();
        assertEquals(2, sent.size());
        assertArrayEquals(mock.history().get(0).key(), mock.history().get(1).key());
        assertEquals("t", mock.history().get(0).topic());
        assertEquals(72, mock.history().get(0).value().length);
    }
}
