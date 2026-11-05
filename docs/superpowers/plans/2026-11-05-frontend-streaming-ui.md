# Frontend Streaming UI & Dynamic Subscription Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a decoupled, optional web streaming module in Java (`market.web`) that bridges the Aeron SBE message bus to browsers via Server-Sent Events (SSE), provides REST APIs for dynamic stock watchlist additions/removals, and serves a modern financial terminal dashboard with live price flashes, latency gauges, and TradingView Lightweight Charts.

**Architecture:** An optional `MarketHttpServer` (disabled by default, enabled via `--enable-ui`) hosts a zero-dependency SSE stream and watchlist API. An Aeron `MarketStreamBroadcaster` decodes SBE ticks from the IPC bus, computes nanosecond transport latency, and broadcasts JSON events to connected web clients. The responsive web dashboard (`index.html`) plots live ticks on TradingView charts and allows users to add new tickers dynamically without restarting the application.

**Tech Stack:** Java 25, embedded `com.sun.net.httpserver.HttpServer`, Server-Sent Events (`text/event-stream`), Real Logic Aeron & SBE, TradingView Lightweight Charts (v4), JUnit 5, AssertJ.

**Spec:** [`docs/superpowers/specs/2026-11-05-frontend-streaming-design.md`](file:///C:/Workspace/git/track-my-market/docs/superpowers/specs/2026-11-05-frontend-streaming-design.md)

## Global Constraints
- Target Java version: Java 25.
- Headless by default: The web server and UI remain completely disabled unless `--enable-ui` or `market.ui.enabled=true` / `MARKET_UI_ENABLED=true` is set.
- Zero npm/Node.js dependencies: Static assets bundled directly in `src/main/resources/web/`.
- Nginx reverse-proxy compatibility: Configurable bind address, port, and context path (e.g. `/` or `/market/`). All frontend API calls use relative paths.
- Resilient client connections: Broken/closed browser connections must be pruned automatically without crashing the Aeron subscription thread.

## Review Focus
1. SSE Connection Management: Ensuring dead browser connections (closed tabs) are cleaned up gracefully without throwing unhandled `IOException` during broadcast loops.
2. Initial State Handshake: Confirming that newly connected SSE clients receive the initial snapshot of known quotes immediately upon connection so the UI doesn't sit blank outside market hours.
3. Thread-Safe Dynamic Watchlist: Ensuring adding or removing symbols via `POST /api/symbols` is thread-safe and triggers an immediate asynchronous poll in `YahooFinanceProvider`.
4. Nginx Reverse Proxy Headers: Confirming `X-Accel-Buffering: no` and `Cache-Control: no-cache` are present on SSE responses to prevent intermediate proxy buffering.
5. Headless Mode Invariance: Verifying that when `--enable-ui` is absent, zero network ports are bound and the core ingestion daemon runs unchanged.

---

### Task 1: Aeron-to-SSE Broadcaster Bridge & Quote Cache

**Files:**
- Create: `src/main/java/market/web/MarketStreamBroadcaster.java`
- Test: `src/test/java/market/web/MarketStreamBroadcasterTest.java`

**Interfaces:**
- Consumes: `io.aeron.logbuffer.FragmentHandler`, `market.sbe.MarketTickDecoder`
- Produces: `MarketStreamBroadcaster`:
  - `void registerClient(OutputStream clientOut)`
  - `void unregisterClient(OutputStream clientOut)`
  - `void onFragment(DirectBuffer buffer, int offset, int length, Header header)`
  - `String getInitialSnapshotJson()`
  - `int getActiveClientCount()`

- [ ] **Step 1: Write the failing Broadcaster test**

Create `src/test/java/market/web/MarketStreamBroadcasterTest.java`:
```java
package market.web;

import market.sbe.MarketTickEncoder;
import market.sbe.MessageHeaderEncoder;
import market.sbe.ModelProvider;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

class MarketStreamBroadcasterTest {

    @Test
    void shouldDecodeSbeTickAndBroadcastSseEventToRegisteredClients() {
        MarketStreamBroadcaster broadcaster = new MarketStreamBroadcaster();
        ByteArrayOutputStream clientOut = new ByteArrayOutputStream();

        broadcaster.registerClient(clientOut);
        assertThat(broadcaster.getActiveClientCount()).isEqualTo(1);

        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        new MarketTickEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .price(25005000L) // 2500.50
            .quoteTimestampNs(1730790000000000000L)
            .ingressTimestampNs(System.currentTimeMillis() * 1_000_000L)
            .fetchDurationNs(10_000_000L)
            .provider(ModelProvider.YAHOO)
            .symbol().value("RELIANCE.NS");

        broadcaster.onFragment(buffer, 0, 256, null);

        String output = clientOut.toString();
        assertThat(output).contains("event: tick\n");
        assertThat(output).contains("\"symbol\":\"RELIANCE.NS\"");
        assertThat(output).contains("\"price\":2500.5");

        String snapshot = broadcaster.getInitialSnapshotJson();
        assertThat(snapshot).contains("RELIANCE.NS");
        assertThat(snapshot).contains("2500.5");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\gradlew.bat test --tests market.web.MarketStreamBroadcasterTest`
Expected: FAIL (class does not exist).

- [ ] **Step 3: Implement `MarketStreamBroadcaster`**

- Maintain `CopyOnWriteArrayList<OutputStream> clients` and `ConcurrentHashMap<String, MarketTickView> latestQuotes`.
- In `onFragment(...)`: decode `MarketTickDecoder`, compute `busLatencyUs = (nowEpochNs - ingressNs) / 1000.0`, build JSON string, write `event: tick\ndata: <JSON>\n\n`, and flush to each client. Remove clients that throw `IOException`.
- Implement `getInitialSnapshotJson()` returning a JSON array of all latest quotes.

- [ ] **Step 4: Run test to verify it passes**

Run: `.\gradlew.bat test --tests market.web.MarketStreamBroadcasterTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/market/web/MarketStreamBroadcaster.java src/test/java/market/web/MarketStreamBroadcasterTest.java
git commit -m "feat: implement market stream broadcaster bridging aeron to sse"
```

---

### Task 2: Dynamic Watchlist Controller & Symbol Subscription API

**Files:**
- Modify: `src/main/java/market/provider/MarketDataProvider.java` (add `unsubscribe(String symbol)` and `List<String> getSubscribedSymbols()`)
- Modify: `src/main/java/market/provider/yahoo/YahooFinanceProvider.java` (implement dynamic symbol addition/removal and query)
- Create: `src/main/java/market/web/DynamicWatchlistController.java`
- Test: `src/test/java/market/web/DynamicWatchlistControllerTest.java`

**Interfaces:**
- Consumes: `MarketDataProvider`
- Produces: `DynamicWatchlistController`:
  - `String handleGetSymbolsJson()`
  - `String handleAddSymbolJson(String requestBody)`
  - `String handleRemoveSymbolJson(String symbol)`

- [ ] **Step 1: Write the failing Watchlist Controller test**

Create `src/test/java/market/web/DynamicWatchlistControllerTest.java`:
```java
package market.web;

import market.provider.yahoo.YahooFinanceProvider;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class DynamicWatchlistControllerTest {

    @Test
    void shouldAddAndRemoveSymbolsDynamically() {
        YahooFinanceProvider provider = new YahooFinanceProvider();
        provider.subscribe(List.of("RELIANCE.NS", "TCS.NS"));

        DynamicWatchlistController controller = new DynamicWatchlistController(provider);

        String initialSymbols = controller.handleGetSymbolsJson();
        assertThat(initialSymbols).contains("RELIANCE.NS", "TCS.NS");

        String addedResponse = controller.handleAddSymbolJson("{\"symbol\":\"INFY.NS\"}");
        assertThat(addedResponse).contains("INFY.NS");
        assertThat(provider.getSubscribedSymbols()).contains("INFY.NS");

        String removedResponse = controller.handleRemoveSymbolJson("TCS.NS");
        assertThat(removedResponse).doesNotContain("TCS.NS");
        assertThat(provider.getSubscribedSymbols()).doesNotContain("TCS.NS");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\gradlew.bat test --tests market.web.DynamicWatchlistControllerTest`
Expected: FAIL.

- [ ] **Step 3: Update `MarketDataProvider` and implement `DynamicWatchlistController`**

- In `MarketDataProvider.java`, add default methods: `List<String> getSubscribedSymbols()` and `void unsubscribe(String symbol)`.
- In `YahooFinanceProvider.java`, implement `unsubscribe(String symbol)` and `getSubscribedSymbols()`.
- Implement `DynamicWatchlistController` with input validation (auto-uppercase, regex `^[A-Z0-9_.-]+$`), JSON parsing, and response formatting.

- [ ] **Step 4: Run test to verify it passes**

Run: `.\gradlew.bat test --tests market.web.DynamicWatchlistControllerTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/market/provider/MarketDataProvider.java src/main/java/market/provider/yahoo/YahooFinanceProvider.java src/main/java/market/web/DynamicWatchlistController.java src/test/java/market/web/DynamicWatchlistControllerTest.java
git commit -m "feat: implement dynamic watchlist controller and symbol subscription api"
```

---

### Task 3: Embedded Web Server & Context Path Routing

**Files:**
- Create: `src/main/java/market/web/MarketHttpServer.java`
- Test: `src/test/java/market/web/MarketHttpServerTest.java`

**Interfaces:**
- Consumes: `MarketStreamBroadcaster`, `DynamicWatchlistController`
- Produces: `MarketHttpServer`:
  - `void start()`
  - `void stop()`
  - `int getPort()`
  - `String getContextPath()`

- [ ] **Step 1: Write the failing MarketHttpServer test**

Create `src/test/java/market/web/MarketHttpServerTest.java`:
```java
package market.web;

import market.provider.yahoo.YahooFinanceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MarketHttpServerTest {
    private MarketHttpServer server;
    private YahooFinanceProvider provider;
    private MarketStreamBroadcaster broadcaster;

    @BeforeEach
    void setUp() throws Exception {
        provider = new YahooFinanceProvider();
        provider.subscribe(List.of("RELIANCE.NS"));
        broadcaster = new MarketStreamBroadcaster();
        DynamicWatchlistController controller = new DynamicWatchlistController(provider);

        server = new MarketHttpServer("127.0.0.1", 0, "/market", broadcaster, controller);
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
    }

    @Test
    void shouldServeSymbolsApiUnderConfiguredContextPath() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/market/api/symbols"))
            .GET()
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("RELIANCE.NS");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\gradlew.bat test --tests market.web.MarketHttpServerTest`
Expected: FAIL.

- [ ] **Step 3: Implement `MarketHttpServer`**

- Configure `HttpServer.create(new InetSocketAddress(bindHost, port), 0)`.
- Set executor to `Executors.newVirtualThreadPerTaskExecutor()` (or cached thread pool).
- Wire routes under `contextPath`:
  - `{contextPath}/` and `{contextPath}/index.html` -> static HTML loader.
  - `{contextPath}/api/stream` -> sets headers `Content-Type: text/event-stream`, `Cache-Control: no-cache`, `X-Accel-Buffering: no`, and registers exchange output stream with `broadcaster`.
  - `{contextPath}/api/symbols` -> routes GET, POST, DELETE to `DynamicWatchlistController`.

- [ ] **Step 4: Run test to verify it passes**

Run: `.\gradlew.bat test --tests market.web.MarketHttpServerTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/market/web/MarketHttpServer.java src/test/java/market/web/MarketHttpServerTest.java
git commit -m "feat: implement embedded http server with context path routing and sse streaming"
```

---

### Task 4: Modern Web Dashboard UI (TradingView Charts & Watchlist Bar)

**Files:**
- Create: `src/main/resources/web/index.html`

**Features:**
- Responsive dark-slate financial terminal interface.
- Real-time stock ticker table with green/red CSS flash on price change.
- Instant search bar with "+ Add Stock" button hooked to `POST ./api/symbols`.
- Interactive TradingView Lightweight Charts canvas streaming live tick prices.
- HUD chips displaying Network Ingestion Latency (ms) and Aeron Bus Latency (µs).

- [ ] **Step 1: Create `src/main/resources/web/index.html`**

Create `src/main/resources/web/index.html` with:
- Dark theme CSS styling (monospace tabular figures, grid layout).
- TradingView Lightweight Charts CDN loader (`https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js`).
- JavaScript module connecting to `const eventSource = new EventSource('./api/stream')`.
- Handles `initial-snapshot` event to render cached quotes immediately.
- Handles `tick` event to update prices, flash table cells green/red, update HUD latency, and append to chart series.
- Interactive add form sending `fetch('./api/symbols', { method: 'POST', body: JSON.stringify({ symbol }) })`.

- [ ] **Step 2: Verify HTML loads cleanly via `MarketHttpServerTest`**

Add test assertion to verify `GET /market/` returns HTTP 200 with `text/html` and contains "track-my-market".
Run: `.\gradlew.bat test --tests market.web.MarketHttpServerTest`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/main/resources/web/index.html src/test/java/market/web/MarketHttpServerTest.java
git commit -m "feat: add modern trading dashboard ui with lightweight charts and watchlist bar"
```

---

### Task 5: Core Application Integration & Optional UI Flag

**Files:**
- Modify: `src/main/java/market/MarketApp.java`
- Create: `src/test/java/market/web/MarketUiIntegrationTest.java`

**Interfaces:**
- Reads `--enable-ui` CLI argument or `market.ui.enabled=true` property or `MARKET_UI_ENABLED=true` env var.
- When enabled:
  - Instantiates `MarketStreamBroadcaster` and registers it with `AeronEventBus`.
  - Instantiates `MarketHttpServer` on configured port (`market.server.port`, default 8080) and context path (`market.server.context-path`, default `/`).
  - Logs `Live Web Dashboard available at http://localhost:8080/`.
- When disabled (default):
  - Skips server initialization; application runs 100% headless.

- [ ] **Step 1: Write the failing MarketUiIntegrationTest**

Create `src/test/java/market/web/MarketUiIntegrationTest.java`:
```java
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
        Path tempDir = Files.createTempDirectory("ui-integration-");
        int streamId = 4001;

        try (AeronEventBus bus = new AeronEventBus("aeron:ipc", streamId, tempDir.toFile(), tempDir.toFile(), true, new org.agrona.concurrent.YieldingIdleStrategy(), 100)) {
            bus.start();

            YahooFinanceProvider provider = new YahooFinanceProvider();
            provider.subscribe(List.of("RELIANCE.NS"));

            MarketStreamBroadcaster broadcaster = new MarketStreamBroadcaster();
            bus.subscribe(streamId, broadcaster);

            DynamicWatchlistController controller = new DynamicWatchlistController(provider);
            MarketHttpServer server = new MarketHttpServer("127.0.0.1", 0, "/", broadcaster, controller);
            server.start();

            MarketDataProducer producer = new MarketDataProducer(bus, streamId);
            producer.onQuote(new RawMarketQuote("RELIANCE.NS", 2520.0, System.currentTimeMillis() * 1_000_000L, 5_000_000L, "INE002A01018", "6714072", "RELI.NS"));

            for (int i = 0; i < 50; i++) {
                bus.poll(10);
                Thread.sleep(5);
            }

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + server.getPort() + "/api/symbols"))
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\gradlew.bat test --tests market.web.MarketUiIntegrationTest`
Expected: FAIL.

- [ ] **Step 3: Update `MarketApp.java` with optional UI flag**

Add CLI flag check:
```java
boolean enableUi = Arrays.asList(args).contains("--enable-ui")
    || Boolean.parseBoolean(System.getProperty("market.ui.enabled", System.getenv().getOrDefault("MARKET_UI_ENABLED", "false")));
```
If `enableUi` is true: instantiate broadcaster, subscribe to event bus, launch `MarketHttpServer`, and register shutdown hook.

- [ ] **Step 4: Run full test suite and verify build**

Run: `.\gradlew.bat test`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/market/MarketApp.java src/test/java/market/web/MarketUiIntegrationTest.java
git commit -m "feat: complete optional web ui streaming integration with cli flag"
```
