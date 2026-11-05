package market.producer;

import io.aeron.logbuffer.FragmentHandler;
import market.bus.MarketEventBus;
import market.provider.RawMarketQuote;
import market.sbe.MarketTickDecoder;
import market.sbe.MessageHeaderDecoder;
import market.sbe.ModelProvider;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

class MarketDataProducerTest {

    static class CapturingEventBus implements MarketEventBus {
        int publishedStreamId = -1;
        DirectBuffer capturedBuffer;
        int capturedLength = -1;

        @Override
        public void start() {}

        @Override
        public boolean publish(int streamId, DirectBuffer buffer, int offset, int length) {
            this.publishedStreamId = streamId;
            ByteBuffer copy = ByteBuffer.allocate(length);
            buffer.getBytes(offset, copy, length);
            this.capturedBuffer = new UnsafeBuffer(copy);
            this.capturedLength = length;
            return true;
        }

        @Override
        public void subscribe(int streamId, FragmentHandler handler) {}

        @Override
        public int poll(int fragmentLimit) {
            return 0;
        }

        @Override
        public void close() {}
    }

    @Test
    void shouldEncodeRawQuoteAndPublishToEventBus() {
        CapturingEventBus bus = new CapturingEventBus();
        MarketDataProducer producer = new MarketDataProducer(bus, 1001, ModelProvider.YAHOO);

        RawMarketQuote quote = new RawMarketQuote(
            "RELIANCE.NS",
            2500.50,
            1730790000000000000L,
            15_000_000L,
            "INE002A01018",
            "6714072",
            "RELI.NS"
        );

        boolean result = producer.onQuote(quote);
        assertThat(result).isTrue();

        assertThat(bus.publishedStreamId).isEqualTo(1001);
        assertThat(bus.capturedLength).isGreaterThan(0);

        MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        MarketTickDecoder tickDecoder = new MarketTickDecoder();

        tickDecoder.wrapAndApplyHeader(bus.capturedBuffer, 0, headerDecoder);

        assertThat(tickDecoder.price()).isEqualTo(25005000L); // scaled x 10^4
        assertThat(tickDecoder.quoteTimestampNs()).isEqualTo(1730790000000000000L);
        assertThat(tickDecoder.fetchDurationNs()).isEqualTo(15_000_000L);
        assertThat(tickDecoder.provider()).isEqualTo(ModelProvider.YAHOO);

        assertThat(tickDecoder.symbol().value().trim()).isEqualTo("RELIANCE.NS");
        assertThat(tickDecoder.isin().value().trim()).isEqualTo("INE002A01018");
        assertThat(tickDecoder.sedol().value().trim()).isEqualTo("6714072");
        assertThat(tickDecoder.ric().value().trim()).isEqualTo("RELI.NS");
    }
}
