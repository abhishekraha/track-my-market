package market.consumer;

import market.sbe.MarketTickEncoder;
import market.sbe.MessageHeaderEncoder;
import market.sbe.ModelProvider;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class MarketDisplayConsumerTest {

    @Test
    void shouldDecodeMarketTickAndRenderLatencyStats() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(output);
        MarketDisplayConsumer consumer = new MarketDisplayConsumer(printStream);

        AtomicReference<MarketDisplayConsumer.MarketTickView> captured = new AtomicReference<>();
        consumer.setTickListener(captured::set);

        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        MarketTickEncoder tickEncoder = new MarketTickEncoder();

        tickEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .price(25005000L) // 2500.50
            .quoteTimestampNs(1730790000000000000L)
            .ingressTimestampNs(System.currentTimeMillis() * 1_000_000L)
            .fetchDurationNs(12_000_000L)
            .provider(ModelProvider.YAHOO);

        tickEncoder.symbol().value("RELIANCE.NS");
        tickEncoder.isin().value("INE002A01018");
        tickEncoder.sedol().value("6714072");
        tickEncoder.ric().value("RELI.NS");

        consumer.onFragment(buffer, 0, 256, null);

        assertThat(consumer.getTickCount()).isEqualTo(1);
        MarketDisplayConsumer.MarketTickView view = consumer.getLatestTick("RELIANCE.NS");
        assertThat(view).isNotNull();
        assertThat(view.symbol()).isEqualTo("RELIANCE.NS");
        assertThat(view.price()).isEqualTo(2500.50);
        assertThat(view.isin()).isEqualTo("INE002A01018");
        assertThat(view.sedol()).isEqualTo("6714072");
        assertThat(view.ric()).isEqualTo("RELI.NS");
        assertThat(view.fetchDurationNs()).isEqualTo(12_000_000L);
        assertThat(view.busLatencyNs()).isGreaterThanOrEqualTo(0);

        String consoleOutput = output.toString();
        assertThat(consoleOutput).contains("TICK: RELIANCE.NS");
        assertThat(consoleOutput).contains("₹  2,500.50");
        assertThat(consoleOutput).contains("INE002A01018");
        assertThat(consoleOutput).contains("Fetch:   12.0 ms");
    }
}
