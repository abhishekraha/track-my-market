package market.bus;

import market.bus.aeron.AeronEventBus;
import market.sbe.MarketTickDecoder;
import market.sbe.MarketTickEncoder;
import market.sbe.MessageHeaderDecoder;
import market.sbe.MessageHeaderEncoder;
import market.sbe.ModelProvider;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AeronEventBusTest {

    private AeronEventBus bus;

    @BeforeEach
    void setUp() {
        bus = new AeronEventBus();
        bus.start();
    }

    @AfterEach
    void tearDown() {
        if (bus != null) {
            bus.close();
        }
    }

    @Test
    void shouldPublishAndPollRawMessage() {
        int streamId = AeronEventBus.DEFAULT_STREAM_ID;
        AtomicReference<String> receivedPayload = new AtomicReference<>();
        AtomicBoolean received = new AtomicBoolean(false);

        bus.subscribe(streamId, (buffer, offset, length, header) -> {
            byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            receivedPayload.set(new String(bytes, StandardCharsets.UTF_8));
            received.set(true);
        });

        String message = "TRACK_MY_MARKET_TEST_PAYLOAD";
        byte[] payloadBytes = message.getBytes(StandardCharsets.UTF_8);
        UnsafeBuffer buffer = new UnsafeBuffer(payloadBytes);

        boolean published = bus.publish(streamId, buffer, 0, payloadBytes.length);
        assertThat(published).isTrue();

        long deadline = System.currentTimeMillis() + 5000;
        while (!received.get() && System.currentTimeMillis() < deadline) {
            bus.poll(10);
            Thread.onSpinWait();
        }

        assertThat(received.get()).isTrue();
        assertThat(receivedPayload.get()).isEqualTo(message);
    }

    @Test
    void shouldPublishAndPollSbeMarketTickMessage() {
        int streamId = 2001;
        AtomicReference<MarketTickDecoder> receivedTick = new AtomicReference<>();
        AtomicBoolean received = new AtomicBoolean(false);

        bus.subscribe(streamId, (buffer, offset, length, header) -> {
            MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
            MarketTickDecoder decoder = new MarketTickDecoder();
            decoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
            receivedTick.set(decoder);
            received.set(true);
        });

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(256);
        UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

        MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        MarketTickEncoder encoder = new MarketTickEncoder();

        encoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder)
                .price(25005000L)
                .quoteTimestampNs(1730790000000000000L)
                .ingressTimestampNs(1730790000005000000L)
                .fetchDurationNs(5000000L)
                .provider(ModelProvider.YAHOO);

        encoder.symbol().value("RELIANCE.NS");
        encoder.isin().value("INE002A01018");
        encoder.sedol().value("6714072");
        encoder.ric().value("RELI.NS");

        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();

        boolean published = bus.publish(streamId, directBuffer, 0, encodedLength);
        assertThat(published).isTrue();

        long deadline = System.currentTimeMillis() + 5000;
        while (!received.get() && System.currentTimeMillis() < deadline) {
            bus.poll(10);
            Thread.onSpinWait();
        }

        assertThat(received.get()).isTrue();
        MarketTickDecoder decoder = receivedTick.get();
        assertThat(decoder).isNotNull();
        assertThat(decoder.price()).isEqualTo(25005000L);
        assertThat(decoder.quoteTimestampNs()).isEqualTo(1730790000000000000L);
        assertThat(decoder.ingressTimestampNs()).isEqualTo(1730790000005000000L);
        assertThat(decoder.fetchDurationNs()).isEqualTo(5000000L);
        assertThat(decoder.provider()).isEqualTo(ModelProvider.YAHOO);
        assertThat(decoder.symbol().value().trim()).isEqualTo("RELIANCE.NS");
        assertThat(decoder.isin().value().trim()).isEqualTo("INE002A01018");
        assertThat(decoder.sedol().value().trim()).isEqualTo("6714072");
        assertThat(decoder.ric().value().trim()).isEqualTo("RELI.NS");
    }

    @Test
    void shouldDeliverToMultipleSubscribersOnSameStream() {
        int streamId = 3001;
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        bus.subscribe(streamId, (buffer, offset, length, header) -> count1.incrementAndGet());
        bus.subscribe(streamId, (buffer, offset, length, header) -> count2.incrementAndGet());

        byte[] payload = "BROADCAST".getBytes(StandardCharsets.UTF_8);
        UnsafeBuffer buffer = new UnsafeBuffer(payload);

        boolean published = bus.publish(streamId, buffer, 0, payload.length);
        assertThat(published).isTrue();

        long deadline = System.currentTimeMillis() + 5000;
        while ((count1.get() < 1 || count2.get() < 1) && System.currentTimeMillis() < deadline) {
            bus.poll(10);
            Thread.onSpinWait();
        }

        assertThat(count1.get()).isEqualTo(1);
        assertThat(count2.get()).isEqualTo(1);
    }

    @Test
    void shouldHandleMultipleMessages() {
        int streamId = AeronEventBus.DEFAULT_STREAM_ID;
        int messageCount = 50;
        List<Integer> receivedSeq = new ArrayList<>();

        bus.subscribe(streamId, (buffer, offset, length, header) -> {
            int seq = buffer.getInt(offset);
            receivedSeq.add(seq);
        });

        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Integer.BYTES));
        for (int i = 0; i < messageCount; i++) {
            buffer.putInt(0, i);
            boolean published = bus.publish(streamId, buffer, 0, Integer.BYTES);
            assertThat(published).isTrue();
        }

        long deadline = System.currentTimeMillis() + 5000;
        while (receivedSeq.size() < messageCount && System.currentTimeMillis() < deadline) {
            bus.poll(10);
            Thread.onSpinWait();
        }

        assertThat(receivedSeq).hasSize(messageCount);
        for (int i = 0; i < messageCount; i++) {
            assertThat(receivedSeq.get(i)).isEqualTo(i);
        }
    }

    @Test
    void shouldCleanlyCloseAndReleaseResources() {
        assertThat(bus.isStarted()).isTrue();
        assertThat(bus.isClosed()).isFalse();

        bus.close();

        assertThat(bus.isStarted()).isFalse();
        assertThat(bus.isClosed()).isTrue();

        // Repeated close should be idempotent
        bus.close();
        assertThat(bus.isClosed()).isTrue();
    }
}
