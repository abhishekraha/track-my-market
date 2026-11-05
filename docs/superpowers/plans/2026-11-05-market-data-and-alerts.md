# Market Data Ingestion, Aeron/SBE Bus & Alerts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a low-latency market tracking system in Java with Gradle that fetches stock quotes (NSE/BSE via Yahoo Finance), publishes zero-allocation SBE messages onto an Aeron Archiving Media Driver message bus, evaluates edge-triggered price threshold rules, and dispatches Telegram notifications.

**Architecture:** A pluggable `MarketDataProvider` fetches quotes and calculates network latency. A `MarketDataProducer` serializes quotes with institutional identifiers (`isin`, `sedol`, `ric`, `symbol`) into SBE flyweights and publishes them to a pluggable `MarketEventBus` backed by an embedded Aeron `ArchivingMediaDriver`. An `AlertEngine` polls the live bus stream, decodes ticks with zero allocations, evaluates edge-triggered rules, and triggers asynchronous Telegram alerts via `HttpClient`.

**Tech Stack:** Java 25, Gradle (Kotlin DSL), Real Logic Aeron (`aeron-all`, `aeron-archive`), Simple Binary Encoding (`sbe-all`), Java `HttpClient`, JUnit 5, AssertJ.

**Spec:** [`docs/superpowers/specs/2026-11-05-market-data-and-alerts-design.md`](file:///C:/Workspace/git/track-my-market/docs/superpowers/specs/2026-11-05-market-data-and-alerts-design.md)

## Global Constraints
- Target Java version: Java 25.
- Build tool: Gradle with Gradle Wrapper (`gradlew`).
- Zero Demat / No KYC required: default configuration runs out-of-the-box using public Yahoo Finance endpoints without static IP restrictions.
- Precision: All timestamps stored as primitive 64-bit Unix epoch nanoseconds (`int64` / Java `long`). Prices stored as scaled unsigned integers (`uint64` / Java `long`, scaled $\times 10^4$).
- Memory efficiency: Zero heap allocations on the hot path (direct off-heap buffers using Agrona `UnsafeBuffer` and SBE flyweights).

## Review Focus
1. SBE Byte Alignment & Buffer Capacity: Ensuring the `UnsafeBuffer` passed to `MarketTickEncoder` has sufficient capacity for header + body, avoiding buffer overrun exceptions.
2. Aeron IPC Driver Lifecycle: Ensuring the embedded `ArchivingMediaDriver` and `Aeron` client initialize clean media directories and shut down cleanly in tests without leaving locked shared-memory files on Windows.
3. Edge-Trigger State Transitions: Verifying alerts fire exactly once on boundary crossing (`ARMED` -> `TRIGGERED`) and do not spam repeated notifications on subsequent above-threshold ticks.
4. Provider Deduplication: Verifying the Yahoo Finance provider suppresses publishing identical quotes when markets are closed or when prices have not updated.
5. Telegram Markdown Escaping & Error Handling: Ensuring special characters in stock symbols or error responses from Telegram HTTP API do not crash the subscriber polling loop.

---

### Task 1: Gradle Build Scaffolding & SBE Schema Definition

**Files:**
- Create: `build.gradle.kts`
- Create: `settings.gradle.kts`
- Create: `gradle/wrapper/gradle-wrapper.properties`
- Create: `src/main/resources/market-data-schema.xml`
- Test: `src/test/java/market/sbe/MarketTickSbeTest.java`

**Interfaces:**
- Produces: Generated SBE classes in package `market.sbe`:
  - `MessageHeaderEncoder` / `MessageHeaderDecoder`
  - `MarketTickEncoder` / `MarketTickDecoder`
  - `ModelProvider` enum (`YAHOO`, `ZERODHA`, `DHAN`, `SIMULATED`)

- [ ] **Step 1: Create Gradle configuration and wrapper settings**

Write `settings.gradle.kts` with `rootProject.name = "track-my-market"`.
Write `build.gradle.kts` defining:
- Java 25 toolchain
- Dependencies: `io.aeron:aeron-all:1.46.7`, `io.aeron:aeron-archive:1.46.7`, `uk.co.real-logic:sbe-all:1.33.0`, `org.junit.jupiter:junit-jupiter:5.11.3`, `org.assertj:assertj-core:3.26.3`.
- SBE code generation task running `uk.co.real_logic.sbe.SbeTool` against `src/main/resources/market-data-schema.xml` outputting to `build/generated/sources/sbe/main/java`, added to `sourceSets["main"].java.srcDir`.

- [ ] **Step 2: Generate Gradle wrapper scripts**

Run: `gradle wrapper` (or bootstrap wrapper scripts).
Expected: `gradlew` and `gradlew.bat` present.

- [ ] **Step 3: Define SBE schema XML**

Create `src/main/resources/market-data-schema.xml` with:
- `package="market.sbe"`, `id="1"`, `version="0"`
- Types: `uint64` price (scaled $\times 10^4$), `int64` timestamps, `fixedString16` for `symbol` and `ric`, `fixedString12` for `isin`, `fixedString7` for `sedol`, `ModelProvider` enum.
- Message: `MarketTick` (`id="1"`).

- [ ] **Step 4: Write the failing SBE serialization test**

Create `src/test/java/market/sbe/MarketTickSbeTest.java`:
```java
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
        encoder.putSymbol("RELIANCE.NS".getBytes(), 0);
        encoder.putIsin("INE002A01018".getBytes(), 0);
        encoder.putSedol("6714072".getBytes(), 0);
        encoder.putRic("RELI.NS".getBytes(), 0);

        MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        MarketTickDecoder decoder = new MarketTickDecoder();
        decoder.wrapAndApplyHeader(directBuffer, 0, headerDecoder);

        assertThat(decoder.price()).isEqualTo(25005000L);
        assertThat(decoder.quoteTimestampNs()).isEqualTo(1730790000000000000L);
        assertThat(decoder.fetchDurationNs()).isEqualTo(5000000L);
        assertThat(decoder.provider()).isEqualTo(ModelProvider.YAHOO);
        
        byte[] symbolBytes = new byte[16];
        decoder.getSymbol(symbolBytes, 0);
        assertThat(new String(symbolBytes).trim()).isEqualTo("RELIANCE.NS");

        byte[] isinBytes = new byte[12];
        decoder.getIsin(isinBytes, 0);
        assertThat(new String(isinBytes).trim()).isEqualTo("INE002A01018");

        byte[] sedolBytes = new byte[7];
        decoder.getSedol(sedolBytes, 0);
        assertThat(new String(sedolBytes).trim()).isEqualTo("6714072");

        byte[] ricBytes = new byte[16];
        decoder.getRic(ricBytes, 0);
        assertThat(new String(ricBytes).trim()).isEqualTo("RELI.NS");
    }
}
```

- [ ] **Step 5: Run test to verify it compiles and passes with SBE generation**

Run: `./gradlew test --tests market.sbe.MarketTickSbeTest`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add build.gradle.kts settings.gradle.kts src/main/resources/market-data-schema.xml src/test/java/market/sbe/MarketTickSbeTest.java
git commit -m "feat: add gradle build and sbe market tick schema"
```

---

### Task 2: Pluggable Event Bus Interface & Aeron Implementation

**Files:**
- Create: `src/main/java/market/bus/MarketEventBus.java`
- Create: `src/main/java/market/bus/aeron/AeronEventBus.java`
- Test: `src/test/java/market/bus/AeronEventBusTest.java`

**Interfaces:**
- Consumes: `org.agrona.DirectBuffer`, `io.aeron.logbuffer.FragmentHandler`
- Produces: `market.bus.MarketEventBus` interface:
  - `void start()`
  - `boolean publish(int streamId, DirectBuffer buffer, int offset, int length)`
  - `void subscribe(int streamId, FragmentHandler handler)`
  - `int poll(int fragmentLimit)`
  - `void close()`

- [ ] **Step 1: Write the failing AeronEventBus integration test**

Create `src/test/java/market/bus/AeronEventBusTest.java`:
```java
package market.bus;

import io.aeron.logbuffer.FragmentHandler;
import market.bus.aeron.AeronEventBus;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThat;

class AeronEventBusTest {
    private Path tempDir;
    private AeronEventBus bus;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("aeron-test-");
        bus = new AeronEventBus("aeron:ipc", tempDir.toString());
        bus.start();
    }

    @AfterEach
    void tearDown() {
        if (bus != null) bus.close();
    }

    @Test
    void shouldPublishAndReceiveMessageOverAeron() {
        int streamId = 1001;
        AtomicInteger receivedCount = new AtomicInteger();

        bus.subscribe(streamId, (buffer, offset, length, header) -> {
            byte[] data = new byte[length];
            buffer.getBytes(offset, data);
            if ("PING".equals(new String(data))) {
                receivedCount.incrementAndGet();
            }
        });

        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(32));
        byte[] payload = "PING".getBytes();
        buffer.putBytes(0, payload);

        boolean published = false;
        for (int i = 0; i < 100 && !published; i++) {
            published = bus.publish(streamId, buffer, 0, payload.length);
            if (!published) {
                try { Thread.sleep(5); } catch (InterruptedException ignored) {}
            }
        }
        assertThat(published).isTrue();

        for (int i = 0; i < 100 && receivedCount.get() == 0; i++) {
            bus.poll(10);
            try { Thread.sleep(5); } catch (InterruptedException ignored) {}
        }
        assertThat(receivedCount.get()).isEqualTo(1);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests market.bus.AeronEventBusTest`
Expected: FAIL (classes do not exist).

- [ ] **Step 3: Implement `MarketEventBus` and `AeronEventBus`**

Create `src/main/java/market/bus/MarketEventBus.java`:
Define the generic bus contract (`start`, `publish`, `subscribe`, `poll`, `close`).

Create `src/main/java/market/bus/aeron/AeronEventBus.java`:
- Configure embedded `ArchivingMediaDriver.Context` with media directory and archive directory.
- Launch `ArchivingMediaDriver`.
- Connect `Aeron` client.
- Add `Publication` and `Subscription` matching the channel and streamId.
- Implement zero-allocation `publish` with retry/backoff on `Publication.ADMIN_ACTION` or `BACK_PRESSURED`.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests market.bus.AeronEventBusTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/market/bus/MarketEventBus.java src/main/java/market/bus/aeron/AeronEventBus.java src/test/java/market/bus/AeronEventBusTest.java
git commit -m "feat: implement aeroneventbus with embedded archiving media driver"
```

---

### Task 3: Pluggable Market Data Provider Interface & Yahoo Finance Adapter

**Files:**
- Create: `src/main/java/market/provider/RawMarketQuote.java`
- Create: `src/main/java/market/provider/MarketDataProvider.java`
- Create: `src/main/java/market/provider/yahoo/YahooFinanceProvider.java`
- Test: `src/test/java/market/provider/YahooFinanceProviderTest.java`

**Interfaces:**
- Produces: `RawMarketQuote` record:
  - `symbol: String`
  - `price: double`
  - `quoteTimestampEpochNs: long`
  - `fetchDurationNs: long`
  - `isin: String`
  - `sedol: String`
  - `ric: String`
- Produces: `MarketDataProvider` interface:
  - `void start()`
  - `void stop()`
  - `void subscribe(List<String> symbols)`
  - `void registerTickListener(Consumer<RawMarketQuote> listener)`

- [ ] **Step 1: Write the failing YahooFinanceProvider parser and deduplication test**

Create `src/test/java/market/provider/YahooFinanceProviderTest.java`:
```java
package market.provider;

import market.provider.yahoo.YahooFinanceProvider;
import org.junit.jupiter.api.Test;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class YahooFinanceProviderTest {
    @Test
    void shouldParseYahooJsonQuoteAndNotifyListener() {
        String mockJsonResponse = """
        {
          "chart": {
            "result": [{
              "meta": {
                "symbol": "RELIANCE.NS",
                "regularMarketPrice": 2500.50,
                "regularMarketTime": 1730790000
              }
            }]
          }
        }
        """;

        List<RawMarketQuote> quotes = new ArrayList<>();
        YahooFinanceProvider provider = new YahooFinanceProvider();
        provider.registerTickListener(quotes::add);

        provider.parseAndEmit("RELIANCE.NS", mockJsonResponse, 5_000_000L);

        assertThat(quotes).hasSize(1);
        RawMarketQuote quote = quotes.get(0);
        assertThat(quote.symbol()).isEqualTo("RELIANCE.NS");
        assertThat(quote.price()).isEqualTo(2500.50);
        assertThat(quote.quoteTimestampEpochNs()).isEqualTo(1730790000L * 1_000_000_000L);
        assertThat(quote.fetchDurationNs()).isEqualTo(5_000_000L);
    }

    @Test
    void shouldSuppressDuplicateQuotes() {
        String mockResponse = """
        {
          "chart": {
            "result": [{
              "meta": {
                "symbol": "TCS.NS",
                "regularMarketPrice": 3900.00,
                "regularMarketTime": 1730790000
              }
            }]
          }
        }
        """;

        List<RawMarketQuote> quotes = new ArrayList<>();
        YahooFinanceProvider provider = new YahooFinanceProvider();
        provider.registerTickListener(quotes::add);

        provider.parseAndEmit("TCS.NS", mockResponse, 2_000_000L);
        provider.parseAndEmit("TCS.NS", mockResponse, 2_000_000L); // Duplicate

        assertThat(quotes).hasSize(1); // Deduped!
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests market.provider.YahooFinanceProviderTest`
Expected: FAIL (classes do not exist).

- [ ] **Step 3: Implement `RawMarketQuote`, `MarketDataProvider`, and `YahooFinanceProvider`**

- `RawMarketQuote`: Java record carrying symbol, price, timestamps, identifiers.
- `YahooFinanceProvider`:
  - Contains symbol metadata lookup table for `isin`, `sedol`, `ric`.
  - Maintains `Map<String, Long> lastQuoteTimes` to suppress duplicate ticks.
  - Implements `parseAndEmit` to extract `regularMarketPrice` and `regularMarketTime`.
  - Runs periodic `ScheduledExecutorService` issuing asynchronous `HttpClient.sendAsync()` requests to `https://query1.finance.yahoo.com/v8/finance/chart/{symbol}`.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests market.provider.YahooFinanceProviderTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/market/provider/RawMarketQuote.java src/main/java/market/provider/MarketDataProvider.java src/main/java/market/provider/yahoo/YahooFinanceProvider.java src/test/java/market/provider/YahooFinanceProviderTest.java
git commit -m "feat: implement market data provider and yahoo finance adapter with deduplication"
```

---

### Task 4: Market Data Producer (SBE Encoder & Aeron Bus Publisher)

**Files:**
- Create: `src/main/java/market/producer/MarketDataProducer.java`
- Test: `src/test/java/market/producer/MarketDataProducerTest.java`

**Interfaces:**
- Consumes: `RawMarketQuote` from Task 3, `MarketEventBus` from Task 2, `MarketTickEncoder` from Task 1.
- Produces: `MarketDataProducer`:
  - `void onQuote(RawMarketQuote quote)`
  - Serializes `RawMarketQuote` to SBE binary direct buffer and publishes to `MarketEventBus`.

- [ ] **Step 1: Write the failing MarketDataProducer test**

Create `src/test/java/market/producer/MarketDataProducerTest.java`:
```java
package market.producer;

import market.bus.MarketEventBus;
import market.provider.RawMarketQuote;
import market.sbe.MarketTickDecoder;
import market.sbe.MessageHeaderDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class MarketDataProducerTest {
    @Test
    void shouldEncodeQuoteAndPublishToEventBus() {
        MarketEventBus mockBus = mock(MarketEventBus.class);
        when(mockBus.publish(anyInt(), any(), anyInt(), anyInt())).thenReturn(true);

        MarketDataProducer producer = new MarketDataProducer(mockBus, 1001);

        RawMarketQuote quote = new RawMarketQuote(
            "RELIANCE.NS",
            2500.50,
            1730790000000000000L,
            4_500_000L,
            "INE002A01018",
            "6714072",
            "RELI.NS"
        );

        producer.onQuote(quote);

        ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(mockBus).publish(eq(1001), bufferCaptor.capture(), eq(0), lengthCaptor.capture());

        DirectBuffer captured = bufferCaptor.getValue();
        MarketTickDecoder decoder = new MarketTickDecoder();
        decoder.wrapAndApplyHeader(captured, 0, new MessageHeaderDecoder());

        assertThat(decoder.price()).isEqualTo(25005000L); // scaled x 10^4
        assertThat(decoder.fetchDurationNs()).isEqualTo(4_500_000L);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests market.producer.MarketDataProducerTest`
Expected: FAIL (class does not exist).

- [ ] **Step 3: Implement `MarketDataProducer`**

- Allocate reusable thread-safe off-heap direct buffer (`UnsafeBuffer`).
- Pre-instantiate `MessageHeaderEncoder` and `MarketTickEncoder`.
- In `onQuote()`: wrap buffer, encode header, set `price = (long)(quote.price() * 10000)`, set timestamps, fixed-length strings (`symbol`, `isin`, `sedol`, `ric`), and call `bus.publish(streamId, buffer, 0, encodedLength)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests market.producer.MarketDataProducerTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/market/producer/MarketDataProducer.java src/test/java/market/producer/MarketDataProducerTest.java
git commit -m "feat: implement market data producer with sbe encoding"
```

---

### Task 5: Alert Rule Engine (Edge-Triggered Evaluation)

**Files:**
- Create: `src/main/java/market/alert/AlertDirection.java`
- Create: `src/main/java/market/alert/AlertState.java`
- Create: `src/main/java/market/alert/AlertRule.java`
- Create: `src/main/java/market/alert/AlertEvent.java`
- Create: `src/main/java/market/alert/AlertEngine.java`
- Test: `src/test/java/market/alert/AlertEngineTest.java`

**Interfaces:**
- Consumes: Aeron `FragmentHandler`, SBE `MarketTickDecoder`
- Produces: `AlertEngine`:
  - `void addRule(AlertRule rule)`
  - `void registerAlertListener(Consumer<AlertEvent> listener)`
  - `void onFragment(DirectBuffer buffer, int offset, int length, Header header)` (implements `FragmentHandler`)

- [ ] **Step 1: Write the failing AlertEngine edge-trigger test**

Create `src/test/java/market/alert/AlertEngineTest.java`:
```java
package market.alert;

import market.sbe.MarketTickEncoder;
import market.sbe.MessageHeaderEncoder;
import market.sbe.ModelProvider;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class AlertEngineTest {
    private UnsafeBuffer encodeTick(String symbol, long scaledPrice) {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        new MarketTickEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .price(scaledPrice)
            .quoteTimestampNs(1730790000000000000L)
            .ingressTimestampNs(1730790000005000000L)
            .fetchDurationNs(5000000L)
            .provider(ModelProvider.YAHOO)
            .putSymbol(symbol.getBytes(), 0)
            .putIsin("INE002A01018".getBytes(), 0)
            .putSedol("6714072".getBytes(), 0)
            .putRic("RELI.NS".getBytes(), 0);
        return buffer;
    }

    @Test
    void shouldTriggerOnlyOnceWhenPriceCrossesThreshold() {
        AlertEngine engine = new AlertEngine();
        List<AlertEvent> alerts = new ArrayList<>();
        engine.registerAlertListener(alerts::add);

        // Rule: Alert when RELIANCE.NS crosses ABOVE 2500.00
        AlertRule rule = new AlertRule("rule-1", "RELIANCE.NS", 2500.00, AlertDirection.ABOVE, Duration.ofMinutes(5));
        engine.addRule(rule);

        // 1. Tick at 2490.00 (below target) -> No alert
        engine.onFragment(encodeTick("RELIANCE.NS", 24900000L), 0, 256, null);
        assertThat(alerts).isEmpty();

        // 2. Tick crosses to 2505.00 (above target) -> Triggered!
        engine.onFragment(encodeTick("RELIANCE.NS", 25050000L), 0, 256, null);
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).currentPrice()).isEqualTo(2505.00);

        // 3. Tick stays at 2510.00 (still above) -> Should NOT fire again (edge-triggered)
        engine.onFragment(encodeTick("RELIANCE.NS", 25100000L), 0, 256, null);
        assertThat(alerts).hasSize(1);

        // 4. Tick drops back below to 2495.00 -> Re-arms
        engine.onFragment(encodeTick("RELIANCE.NS", 24950000L), 0, 256, null);
        assertThat(rule.getState()).isEqualTo(AlertState.ARMED);

        // 5. Tick crosses above again to 2502.00 -> Fires second alert!
        engine.onFragment(encodeTick("RELIANCE.NS", 25020000L), 0, 256, null);
        assertThat(alerts).hasSize(2);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests market.alert.AlertEngineTest`
Expected: FAIL (classes do not exist).

- [ ] **Step 3: Implement `AlertRule`, `AlertEvent`, and `AlertEngine`**

- `AlertDirection`: Enum (`ABOVE`, `BELOW`).
- `AlertState`: Enum (`ARMED`, `TRIGGERED`).
- `AlertRule`: Rule properties, mutable `state`, and `lastTriggeredAt`.
- `AlertEngine`:
  - Implements `FragmentHandler`.
  - Uses `MarketTickDecoder` to parse incoming buffer with zero heap allocations.
  - Matches symbol, compares `price / 10000.0` to `targetPrice`.
  - Re-arms when price retreats across threshold; fires and transitions to `TRIGGERED` when crossing.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests market.alert.AlertEngineTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/market/alert/ src/test/java/market/alert/AlertEngineTest.java
git commit -m "feat: implement edge-triggered alert engine"
```

---

### Task 6: Telegram Notifier

**Files:**
- Create: `src/main/java/market/alert/TelegramNotifier.java`
- Test: `src/test/java/market/alert/TelegramNotifierTest.java`

**Interfaces:**
- Consumes: `AlertEvent` from Task 5.
- Produces: `TelegramNotifier`:
  - `CompletableFuture<Boolean> sendAlert(AlertEvent event)`
  - Formats markdown message and issues async HTTP POST to `https://api.telegram.org/bot<TOKEN>/sendMessage`.

- [ ] **Step 1: Write the failing TelegramNotifier formatting and dispatch test**

Create `src/test/java/market/alert/TelegramNotifierTest.java`:
```java
package market.alert;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.assertThat;

class TelegramNotifierTest {
    private HttpServer mockServer;
    private AtomicReference<String> receivedBody = new AtomicReference<>();

    @BeforeEach
    void setUp() throws Exception {
        mockServer = HttpServer.create(new InetSocketAddress(0), 0);
        mockServer.createContext("/botTEST_TOKEN/sendMessage", exchange -> {
            receivedBody.set(new String(exchange.getRequestBody().readAllBytes()));
            byte[] response = "{\"ok\":true}".getBytes();
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        });
        mockServer.start();
    }

    @AfterEach
    void tearDown() {
        mockServer.stop(0);
    }

    @Test
    void shouldFormatAndDispatchTelegramAlert() {
        int port = mockServer.getAddress().getPort();
        String apiBase = "http://localhost:" + port;

        TelegramNotifier notifier = new TelegramNotifier(
            "TEST_TOKEN",
            "12345678",
            HttpClient.newHttpClient(),
            apiBase
        );

        AlertEvent event = new AlertEvent(
            "rule-1",
            "RELIANCE.NS",
            2500.00,
            2505.50,
            AlertDirection.ABOVE,
            Instant.parse("2026-11-05T08:30:00Z")
        );

        boolean success = notifier.sendAlert(event).join();
        assertThat(success).isTrue();
        assertThat(receivedBody.get()).contains("RELIANCE.NS");
        assertThat(receivedBody.get()).contains("2505.5");
        assertThat(receivedBody.get()).contains("12345678");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests market.alert.TelegramNotifierTest`
Expected: FAIL (class does not exist).

- [ ] **Step 3: Implement `TelegramNotifier`**

- Constructor accepts `botToken`, `chatId`, `HttpClient`, and base URL (defaulting to `https://api.telegram.org`).
- In `sendAlert(AlertEvent)`:
  - Formats message:
    ```
    🚨 *Price Alert:* RELIANCE.NS
    Current Price: ₹2505.50
    Condition: ABOVE ₹2500.00
    Time: 2026-11-05T08:30:00Z
    ```
  - Sends JSON POST request with `parse_mode: Markdown`.
  - Returns `CompletableFuture<Boolean>` completing `true` on 200 OK, `false` with error logging on failure.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests market.alert.TelegramNotifierTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/market/alert/TelegramNotifier.java src/test/java/market/alert/TelegramNotifierTest.java
git commit -m "feat: implement telegram alert notifier via http client"
```

---

### Task 7: End-to-End Bootstrap Application & Integration Test

**Files:**
- Create: `src/main/java/market/MarketApp.java`
- Create: `src/main/java/market/AppConfig.java`
- Test: `src/test/java/market/MarketAppIntegrationTest.java`

**Interfaces:**
- Wires all components: `AeronEventBus`, `YahooFinanceProvider`, `MarketDataProducer`, `AlertEngine`, and `TelegramNotifier`.
- Provides CLI executable entrypoint with graceful shutdown hooks.

- [ ] **Step 1: Write the failing End-to-End integration test**

Create `src/test/java/market/MarketAppIntegrationTest.java`:
```java
package market;

import market.alert.AlertDirection;
import market.alert.AlertEngine;
import market.alert.AlertEvent;
import market.alert.AlertRule;
import market.bus.aeron.AeronEventBus;
import market.producer.MarketDataProducer;
import market.provider.RawMarketQuote;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import static org.assertj.core.api.Assertions.assertThat;

class MarketAppIntegrationTest {
    @Test
    void shouldPropagateQuoteThroughAeronBusToAlertEngine() throws Exception {
        Path tempDir = Files.createTempDirectory("market-app-integration-");
        int streamId = 2001;

        try (AeronEventBus bus = new AeronEventBus("aeron:ipc", tempDir.toString())) {
            bus.start();

            AlertEngine alertEngine = new AlertEngine();
            CopyOnWriteArrayList<AlertEvent> firedAlerts = new CopyOnWriteArrayList<>();
            alertEngine.registerAlertListener(firedAlerts::add);
            alertEngine.addRule(new AlertRule("r1", "INFY.NS", 1800.00, AlertDirection.ABOVE, Duration.ofMinutes(1)));

            // Subscribe AlertEngine to Aeron bus
            bus.subscribe(streamId, alertEngine);

            MarketDataProducer producer = new MarketDataProducer(bus, streamId);

            // Publish quote below target
            producer.onQuote(new RawMarketQuote("INFY.NS", 1790.0, System.nanoTime(), 1_000_000L, "INE009A01021", "6459397", "INFY.NS"));
            for (int i = 0; i < 20; i++) { bus.poll(10); Thread.sleep(5); }
            assertThat(firedAlerts).isEmpty();

            // Publish quote crossing target
            producer.onQuote(new RawMarketQuote("INFY.NS", 1810.0, System.nanoTime(), 1_000_000L, "INE009A01021", "6459397", "INFY.NS"));
            for (int i = 0; i < 50 && firedAlerts.isEmpty(); i++) {
                bus.poll(10);
                Thread.sleep(5);
            }

            assertThat(firedAlerts).hasSize(1);
            assertThat(firedAlerts.get(0).currentPrice()).isEqualTo(1810.0);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests market.MarketAppIntegrationTest`
Expected: FAIL (application entrypoint and wiring not complete).

- [ ] **Step 3: Implement `AppConfig` and `MarketApp`**

- `AppConfig`: Reads symbols, alert rules, and Telegram credentials from environment variables or a configuration file.
- `MarketApp`:
  - Instantiates `AeronEventBus` (with embedded driver).
  - Instantiates `AlertEngine` and wires `TelegramNotifier`.
  - Subscribes `AlertEngine` to `AeronEventBus`.
  - Instantiates `MarketDataProducer` and connects it as tick listener to `YahooFinanceProvider`.
  - Starts provider and enters duty polling loop (`bus.poll(10)`) until SIGINT/Ctrl+C.
  - Registers `Runtime.getRuntime().addShutdownHook` for graceful cleanup.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests market.MarketAppIntegrationTest`
Expected: PASS

- [ ] **Step 5: Run full test suite and verify build**

Run: `./gradlew test`
Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/market/MarketApp.java src/main/java/market/AppConfig.java src/test/java/market/MarketAppIntegrationTest.java
git commit -m "feat: complete application bootstrap and end-to-end integration"
```
