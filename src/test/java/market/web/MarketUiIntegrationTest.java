package market.web;

import market.bus.aeron.AeronEventBus;
import market.producer.MarketDataProducer;
import market.provider.RawMarketQuote;
import market.provider.yahoo.YahooFinanceProvider;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MarketUiIntegrationTest {

    @Test
    void shouldStreamTicksThroughAeronBusToHttpSseEndpointWhenUiEnabled() throws Exception {
        Path mediaDir = Files.createTempDirectory("ui-integ-driver-");
        Path archiveDir = Files.createTempDirectory("ui-integ-archive-");
        int streamId = 4001;

        try (AeronEventBus bus = new AeronEventBus(
                "aeron:ipc",
                streamId,
                mediaDir.toFile(),
                archiveDir.toFile(),
                true,
                new org.agrona.concurrent.YieldingIdleStrategy(),
                100)) {

            bus.start();

            YahooFinanceProvider provider = new YahooFinanceProvider();
            provider.subscribe(List.of("RELIANCE.NS"));

            MarketStreamBroadcaster broadcaster = new MarketStreamBroadcaster();
            bus.subscribe(streamId, broadcaster);

            DynamicWatchlistController controller = new DynamicWatchlistController(provider);
            MarketHttpServer server = new MarketHttpServer("127.0.0.1", 0, "/market", broadcaster, controller);
            server.start();

            MarketDataProducer producer = new MarketDataProducer(bus, streamId);
            producer.onQuote(new RawMarketQuote(
                "RELIANCE.NS",
                2520.0,
                System.currentTimeMillis() * 1_000_000L,
                5_000_000L,
                "INE002A01018",
                "6714072",
                "RELI.NS"
            ));

            for (int i = 0; i < 50; i++) {
                bus.poll(10);
                Thread.sleep(5);
            }

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/symbols"))
                .GET()
                .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).contains("RELIANCE.NS");

            assertThat(broadcaster.getInitialSnapshotJson()).contains("RELIANCE.NS", "2520.0");
            server.stop();
        }
    }
}
