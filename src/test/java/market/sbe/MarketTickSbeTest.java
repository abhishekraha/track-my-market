package market.sbe;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import static org.assertj.core.api.Assertions.assertThat;

class MarketTickSbeTest {
    @Test
    void shouldEncodeAndDecodeMarketTick() {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(256);
        UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);

        MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        MarketTickEncoder encoder = new MarketTickEncoder();

        encoder.wrapAndApplyHeader(directBuffer, 0, headerEncoder)
            .price(25005000L) // 2500.50 * 10^4
            .quoteTimestampNs(1730790000000000000L)
            .ingressTimestampNs(1730790000005000000L)
            .fetchDurationNs(5000000L)
            .provider(ModelProvider.YAHOO);

        encoder.symbol().value("RELIANCE.NS");
        encoder.isin().value("INE002A01018");
        encoder.sedol().value("6714072");
        encoder.ric().value("RELI.NS");

        MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        MarketTickDecoder decoder = new MarketTickDecoder();
        decoder.wrapAndApplyHeader(directBuffer, 0, headerDecoder);

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
}
