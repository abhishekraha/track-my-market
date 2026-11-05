package market.producer;

import market.bus.MarketEventBus;
import market.provider.RawMarketQuote;
import market.sbe.MarketTickEncoder;
import market.sbe.MessageHeaderEncoder;
import market.sbe.ModelProvider;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Objects;

/**
 * Encodes RawMarketQuote instances into zero-allocation SBE binary messages
 * and publishes them onto the MarketEventBus.
 */
public class MarketDataProducer {
    private final MarketEventBus eventBus;
    private final int streamId;
    private final ModelProvider provider;
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final MarketTickEncoder tickEncoder = new MarketTickEncoder();
    private final UnsafeBuffer directBuffer;

    public MarketDataProducer(MarketEventBus eventBus, int streamId) {
        this(eventBus, streamId, ModelProvider.YAHOO);
    }

    public MarketDataProducer(MarketEventBus eventBus, int streamId, ModelProvider provider) {
        this.eventBus = Objects.requireNonNull(eventBus, "eventBus cannot be null");
        this.streamId = streamId;
        this.provider = Objects.requireNonNull(provider, "provider cannot be null");
        this.directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(512));
    }

    public synchronized boolean onQuote(RawMarketQuote quote) {
        if (quote == null) {
            return false;
        }

        Instant now = Instant.now();
        long ingressTimestampNs = now.getEpochSecond() * 1_000_000_000L + now.getNano();
        long scaledPrice = Math.round(quote.price() * 10_000.0);

        tickEncoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder)
            .price(scaledPrice)
            .quoteTimestampNs(quote.quoteTimestampEpochNs())
            .ingressTimestampNs(ingressTimestampNs)
            .fetchDurationNs(quote.fetchDurationNs())
            .provider(provider);

        tickEncoder.symbol().value(safeString(quote.symbol(), 16));
        tickEncoder.isin().value(safeString(quote.isin(), 12));
        tickEncoder.sedol().value(safeString(quote.sedol(), 7));
        tickEncoder.ric().value(safeString(quote.ric(), 16));

        int encodedLength = MessageHeaderEncoder.ENCODED_LENGTH + tickEncoder.encodedLength();
        return eventBus.publish(streamId, directBuffer, 0, encodedLength);
    }

    private static String safeString(String value, int maxLength) {
        if (value == null) {
            return "";
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
