package market.web;

import market.provider.yahoo.YahooFinanceProvider;
import market.sbe.MarketTickEncoder;
import market.sbe.MessageHeaderEncoder;
import market.sbe.ModelProvider;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MarketHttpServerTest {

    private MarketHttpServer server;
    private YahooFinanceProvider provider;
    private MarketStreamBroadcaster broadcaster;
    private DynamicWatchlistController controller;
    private HttpClient client;

    @BeforeEach
    void setUp() throws Exception {
        provider = new YahooFinanceProvider();
        provider.subscribe(List.of("RELIANCE.NS", "TCS.NS"));
        broadcaster = new MarketStreamBroadcaster();
        controller = new DynamicWatchlistController(provider);

        server = new MarketHttpServer("127.0.0.1", 0, "/market", broadcaster, controller);
        server.start();

        client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    @DisplayName("Should expose correct port and normalized context path")
    void shouldExposePortAndContextPath() {
        assertThat(server.getPort()).isGreaterThan(0);
        assertThat(server.getContextPath()).isEqualTo("/market");
    }

    @Test
    @DisplayName("Should serve static HTML on root and index.html under context path")
    void shouldServeStaticHtml() throws Exception {
        HttpRequest rootReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/"))
            .GET()
            .build();
        HttpResponse<String> rootResp = client.send(rootReq, HttpResponse.BodyHandlers.ofString());
        assertThat(rootResp.statusCode()).isEqualTo(200);
        assertThat(rootResp.headers().firstValue("Content-Type")).hasValueSatisfying(ct -> assertThat(ct).contains("text/html"));
        assertThat(rootResp.body()).containsIgnoringCase("Track My Market");

        HttpRequest indexReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/index.html"))
            .GET()
            .build();
        HttpResponse<String> indexResp = client.send(indexReq, HttpResponse.BodyHandlers.ofString());
        assertThat(indexResp.statusCode()).isEqualTo(200);
        assertThat(indexResp.body()).containsIgnoringCase("Track My Market");
    }

    @Test
    @DisplayName("Should serve GET, POST, DELETE symbols API under context path")
    void shouldHandleSymbolsApi() throws Exception {
        // GET symbols
        HttpRequest getReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/symbols"))
            .GET()
            .build();
        HttpResponse<String> getResp = client.send(getReq, HttpResponse.BodyHandlers.ofString());
        assertThat(getResp.statusCode()).isEqualTo(200);
        assertThat(getResp.headers().firstValue("Content-Type")).hasValueSatisfying(ct -> assertThat(ct).contains("application/json"));
        assertThat(getResp.body()).contains("RELIANCE.NS", "TCS.NS");

        // POST symbol
        HttpRequest postReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/symbols"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{\"symbol\":\"INFY.NS\"}"))
            .build();
        HttpResponse<String> postResp = client.send(postReq, HttpResponse.BodyHandlers.ofString());
        assertThat(postResp.statusCode()).isEqualTo(200);
        assertThat(postResp.body()).contains("INFY.NS");

        // DELETE symbol
        HttpRequest deleteReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/symbols/TCS.NS"))
            .DELETE()
            .build();
        HttpResponse<String> deleteResp = client.send(deleteReq, HttpResponse.BodyHandlers.ofString());
        assertThat(deleteResp.statusCode()).isEqualTo(200);
        assertThat(deleteResp.body()).doesNotContain("TCS.NS");
        assertThat(deleteResp.body()).contains("INFY.NS");
    }

    @Test
    @DisplayName("Should return 400 when POST /api/symbols receives invalid payload")
    void shouldReturn400OnInvalidSymbolPayload() throws Exception {
        HttpRequest badReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/symbols"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{\"invalid\":\"key\"}"))
            .build();
        HttpResponse<String> badResp = client.send(badReq, HttpResponse.BodyHandlers.ofString());
        assertThat(badResp.statusCode()).isEqualTo(400);
    }

    @Test
    @DisplayName("Should establish SSE stream with initial snapshot and streaming ticks")
    void shouldEstablishSseStreamAndReceiveInitialSnapshotAndTicks() throws Exception {
        // Pre-populate broadcaster with a quote
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        new MarketTickEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .price(25005000L)
            .quoteTimestampNs(1730790000000000000L)
            .ingressTimestampNs(System.currentTimeMillis() * 1_000_000L)
            .fetchDurationNs(10_000_000L)
            .provider(ModelProvider.YAHOO)
            .symbol().value("RELIANCE.NS");
        broadcaster.onFragment(buffer, 0, 256, null);

        HttpRequest streamReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/stream"))
            .GET()
            .build();

        HttpResponse<java.io.InputStream> streamResp = client.send(streamReq, HttpResponse.BodyHandlers.ofInputStream());
        assertThat(streamResp.statusCode()).isEqualTo(200);
        assertThat(streamResp.headers().firstValue("Content-Type")).hasValueSatisfying(ct -> assertThat(ct).contains("text/event-stream"));
        assertThat(streamResp.headers().firstValue("Cache-Control")).hasValueSatisfying(cc -> assertThat(cc).contains("no-cache"));
        assertThat(streamResp.headers().firstValue("X-Accel-Buffering")).hasValue("no");

        BufferedReader reader = new BufferedReader(new InputStreamReader(streamResp.body(), StandardCharsets.UTF_8));
        String line1 = reader.readLine();
        String line2 = reader.readLine();
        reader.readLine(); // empty line

        assertThat(line1).isEqualTo("event: initial-snapshot");
        assertThat(line2).contains("data: ").contains("RELIANCE.NS").contains("2500.5");

        // Now broadcast a live tick
        new MarketTickEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .price(35000000L)
            .quoteTimestampNs(1730790000000000000L)
            .ingressTimestampNs(System.currentTimeMillis() * 1_000_000L)
            .fetchDurationNs(12_000_000L)
            .provider(ModelProvider.YAHOO)
            .symbol().value("TCS.NS");
        broadcaster.onFragment(buffer, 0, 256, null);

        String tickEvent = reader.readLine();
        String tickData = reader.readLine();
        reader.readLine(); // empty line

        assertThat(tickEvent).isEqualTo("event: tick");
        assertThat(tickData).contains("data: ").contains("TCS.NS").contains("3500.0");

        streamResp.body().close();
    }

    @Test
    @DisplayName("Should return 404 for unknown paths under context")
    void shouldReturn404ForUnknownPaths() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/unknown/path"))
            .GET()
            .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(404);
    }

    @Test
    @DisplayName("Should return 405 for unsupported HTTP methods on endpoints")
    void shouldReturn405ForUnsupportedMethods() throws Exception {
        // POST to /api/stream
        HttpRequest postStreamReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/stream"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
        HttpResponse<Void> postStreamResp = client.send(postStreamReq, HttpResponse.BodyHandlers.discarding());
        assertThat(postStreamResp.statusCode()).isEqualTo(405);

        // PUT to /api/symbols
        HttpRequest putSymbolsReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/symbols"))
            .PUT(HttpRequest.BodyPublishers.noBody())
            .build();
        HttpResponse<Void> putSymbolsResp = client.send(putSymbolsReq, HttpResponse.BodyHandlers.discarding());
        assertThat(putSymbolsResp.statusCode()).isEqualTo(405);
    }

    @Test
    @DisplayName("Should return 400 when DELETE /api/symbols has invalid or missing symbol")
    void shouldReturn400OnInvalidDeleteSymbol() throws Exception {
        // DELETE /api/symbols with empty symbol
        HttpRequest deleteEmptyReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/symbols/"))
            .DELETE()
            .build();
        HttpResponse<String> deleteEmptyResp = client.send(deleteEmptyReq, HttpResponse.BodyHandlers.ofString());
        assertThat(deleteEmptyResp.statusCode()).isEqualTo(400);

        // DELETE with invalid symbol format
        HttpRequest deleteInvalidReq = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/symbols/INVALID$$$SYMBOL"))
            .DELETE()
            .build();
        HttpResponse<String> deleteInvalidResp = client.send(deleteInvalidReq, HttpResponse.BodyHandlers.ofString());
        assertThat(deleteInvalidResp.statusCode()).isEqualTo(400);
    }

    @Test
    @DisplayName("Should normalize context paths properly (e.g. root, trailing slashes)")
    void shouldNormalizeContextPaths() throws Exception {
        MarketHttpServer rootServer = new MarketHttpServer("127.0.0.1", 0, "/", broadcaster, controller);
        rootServer.start();
        try {
            assertThat(rootServer.getContextPath()).isEqualTo("/");
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + rootServer.getPort() + "/api/symbols"))
                .GET()
                .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            assertThat(resp.statusCode()).isEqualTo(200);
            assertThat(resp.body()).contains("RELIANCE.NS");
        } finally {
            rootServer.stop();
        }

        MarketHttpServer slashServer = new MarketHttpServer("127.0.0.1", 0, "/market/", broadcaster, controller);
        slashServer.start();
        try {
            assertThat(slashServer.getContextPath()).isEqualTo("/market");
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + slashServer.getPort() + "/market/api/symbols"))
                .GET()
                .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            assertThat(resp.statusCode()).isEqualTo(200);
            assertThat(resp.body()).contains("RELIANCE.NS");
        } finally {
            slashServer.stop();
        }
    }

    @Test
    @DisplayName("Should serve symbols status API under context path")
    void shouldServeSymbolsStatusEndpoint() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/symbols/status"))
            .GET()
            .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.headers().firstValue("Content-Type").orElse("")).contains("application/json");
        assertThat(resp.body()).contains("RELIANCE.NS");
    }

    @Test
    @DisplayName("Should serve search API under context path")
    void shouldServeSearchEndpoint() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/search?q=test"))
            .GET()
            .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.headers().firstValue("Content-Type").orElse("")).contains("application/json");
        assertThat(resp.body()).startsWith("[").endsWith("]");
    }

    @Test
    @DisplayName("Should serve chart API under context path")
    void shouldServeChartEndpoint() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/chart?symbol=RELIANCE.NS&timeframe=1d"))
            .GET()
            .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.headers().firstValue("Content-Type").orElse("")).contains("application/json");
        assertThat(resp.body()).contains("\"symbol\":\"RELIANCE.NS\"");
        assertThat(resp.body()).contains("\"timeframe\":\"1d\"");
    }

    @Test
    @DisplayName("Should serve FX rates API under context path")
    void shouldServeFxEndpoint() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/fx"))
            .GET()
            .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.headers().firstValue("Content-Type").orElse("")).contains("application/json");
        assertThat(resp.body()).contains("\"base\":\"USD\"");
        assertThat(resp.body()).contains("\"rates\":");
        assertThat(resp.body()).contains("\"INR\":");
        assertThat(resp.body()).contains("\"BRL\":");
        assertThat(resp.body()).contains("\"CNY\":");
    }
}
