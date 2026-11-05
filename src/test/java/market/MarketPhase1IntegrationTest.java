package market;

import market.bus.aeron.AeronEventBus;
import market.consumer.MarketDisplayConsumer;
import market.producer.MarketDataProducer;
import market.provider.RawMarketQuote;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class MarketPhase1IntegrationTest {

    @Test
    void shouldStreamQuotesOverAeronBusAndDecodeWithDisplayConsumer() throws Exception {
        Path mediaDir = Files.createTempDirectory("phase1-driver-");
        Path archiveDir = Files.createTempDirectory("phase1-archive-");
        int streamId = 3001;

        try (AeronEventBus bus = new AeronEventBus(
                "aeron:ipc",
                streamId,
                mediaDir.toFile(),
                archiveDir.toFile(),
                true,
                new org.agrona.concurrent.YieldingIdleStrategy(),
                100)) {

            bus.start();

            MarketDisplayConsumer consumer = new MarketDisplayConsumer();
            CountDownLatch latch = new CountDownLatch(2);
            consumer.setTickListener(view -> latch.countDown());

            bus.subscribe(streamId, consumer);

            MarketDataProducer producer = new MarketDataProducer(bus, streamId);

            long quoteTime = System.currentTimeMillis() * 1_000_000L;

            RawMarketQuote quote1 = new RawMarketQuote(
                "RELIANCE.NS",
                2510.75,
                quoteTime,
                8_000_000L,
                "INE002A01018",
                "6714072",
                "RELI.NS"
            );

            RawMarketQuote quote2 = new RawMarketQuote(
                "TCS.NS",
                3920.00,
                quoteTime + 10_000_000L,
                11_000_000L,
                "INE467B01029",
                "6873587",
                "TCS.NS"
            );

            boolean p1 = producer.onQuote(quote1);
            boolean p2 = producer.onQuote(quote2);

            assertThat(p1).isTrue();
            assertThat(p2).isTrue();

            for (int i = 0; i < 100 && latch.getCount() > 0; i++) {
                bus.poll(10);
                Thread.sleep(5);
            }

            boolean completed = latch.await(2, TimeUnit.SECONDS);
            assertThat(completed).isTrue();
            assertThat(consumer.getTickCount()).isEqualTo(2);

            MarketDisplayConsumer.MarketTickView view1 = consumer.getLatestTick("RELIANCE.NS");
            assertThat(view1).isNotNull();
            assertThat(view1.price()).isEqualTo(2510.75);
            assertThat(view1.isin()).isEqualTo("INE002A01018");
            assertThat(view1.sedol()).isEqualTo("6714072");
            assertThat(view1.ric()).isEqualTo("RELI.NS");
            assertThat(view1.busLatencyNs()).isGreaterThanOrEqualTo(0);

            MarketDisplayConsumer.MarketTickView view2 = consumer.getLatestTick("TCS.NS");
            assertThat(view2).isNotNull();
            assertThat(view2.price()).isEqualTo(3920.00);
            assertThat(view2.isin()).isEqualTo("INE467B01029");
            assertThat(view2.sedol()).isEqualTo("6873587");
            assertThat(view2.ric()).isEqualTo("TCS.NS");
        }
    }
}
