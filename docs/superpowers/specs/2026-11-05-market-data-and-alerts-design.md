# Design Doc: Pluggable Market Data Ingestion, Aeron/SBE Event Bus & Telegram Alerts

**Date:** 2026-11-05  
**Topic:** track-my-market (Subsystem 1: Market Data Ingestion, Aeron/SBE Bus, Alert Engine)  
**Status:** In Review  

---

## 1. Intent & The Why
The goal of `track-my-market` is to monitor stock and index price movements, display live market data, record ultra-precise latency statistics for Grafana visualization, and notify users via Telegram when price rules are met.

This document specifies Subsystem 1:
1. **Pluggable Market Data Fetcher:** Ingests quotes (starting with NSE/BSE via a free Yahoo Finance polling stream, with zero Demat account and zero static IP requirements).
2. **High-Performance Pluggable Event Bus:** Employs **Aeron Archiving Media Driver** with **SBE (Simple Binary Encoding)** for zero-allocation, nanosecond-precision sequenced messaging, while keeping the bus interface abstracted so alternative buses (such as Apache Kafka) can be swapped in.
3. **Alert Engine & Rule Evaluator:** Consumes ticks from the bus, checks configurable price thresholds (e.g., `price >= target` or `price <= target` with cooldown debouncing), and triggers alerts.
4. **Telegram Notifier:** Dispatches formatted alert messages via the Telegram Bot HTTP API.
5. **Observability Readiness:** Nanosecond timestamp fields (`ingressTimestampNs`, `busPublishTimestampNs`, `consumerTimestampNs`) baked into SBE messages to feed Grafana latency metrics in subsequent phases.

---

## 2. Goals, Non-Goals & Anti-Goals

### Goals
- **Gradle Build System:** Structured with Gradle and Gradle Wrapper (`gradlew`), targeting Java 25.
- **SBE Schema & Flyweights:** Binary schema definition (`market-data-schema.xml`) compiled into zero-allocation Java encoders and decoders.
- **Pluggable Event Bus (`MarketEventBus`):**
  - Abstract interface providing `publish(...)` and `subscribe(...)`.
  - Primary implementation: `AeronEventBus` with embedded `ArchivingMediaDriver` (recorded catalog, IPC/UDP stream, microsecond latency).
  - Future-ready adapter interface for `KafkaEventBus`.
- **Pluggable Market Data Provider (`MarketDataProvider`):**
  - Abstract interface providing `subscribe(symbols)` and `start()` / `stop()`.
  - Default adapter: `YahooFinanceProvider` for NSE/BSE stocks (`RELIANCE.NS`, `TCS.NS`, etc.) without Demat or static IP barriers.
- **Alert Engine:** In-memory rule evaluation with cooldown windows to prevent alert spam.
- **Async Telegram Alerts:** Asynchronous notification using Java 25 `HttpClient`.

### Non-Goals
- Automated trade execution or broker order routing.
- Complex technical indicator math (RSI, Bollinger Bands) — strictly price threshold triggers for this phase.
- Distributed cluster coordination (Aeron embedded driver on IPC/UDP is sufficient for single-host low-latency pipelines).

### Anti-Goals
- Tight coupling to proprietary broker APIs or Kafka-specific classes in business logic.
- Blocking thread pools or excessive GC allocations in the tick ingestion and alert processing path.

---

## 3. Constraints
- **Zero Demat / No KYC:** Default setup must run without requiring Demat accounts or paid vendor API subscriptions.
- **No Static IP:** Must operate smoothly over consumer / dynamic IPs.
- **Tooling:** Java 25 + Gradle build.

---

## 4. Approaches & Architecture Decisions

### 4.1 Message Bus: Aeron Archiving Media Driver vs. Kafka
- **Considered:** Apache Kafka vs. Aeron (Archive / Sequencer).
- **Decision:** **Aeron with embedded Archiving Media Driver** as the primary engine because:
  1. Latency is microsecond/nanosecond-scale (zero-copy shared memory IPC), which is essential for true latency stat tracking in Grafana.
  2. Runs fully embedded in the JVM process without requiring external brokers or Docker containers.
  3. Native zero-allocation synergy with SBE direct buffers.
- **Pluggability:** The publisher and subscriber are isolated behind a generic `MarketEventBus` interface. A `KafkaEventBus` can be plugged in later if multi-host cluster distribution is needed.

### 4.2 Data Fetcher: Polled Stream vs. Broker WebSocket
- **Decision:** Pluggable `MarketDataProvider` interface. Default implementation polls Yahoo Finance for NSE/BSE symbols, encodes to SBE, and offers to the bus. Direct broker WebSockets (Dhan, Zerodha, Angel One) can implement the same interface.

---

## 5. System Architecture & Detailed Components

```
+----------------------------------------------------------------------------------+
|                               track-my-market                                    |
|                                                                                  |
|  +----------------------------+                                                  |
|  |   YahooFinanceProvider     |  (Pluggable MarketDataProvider)                  |
|  |  (NSE/BSE Polling Stream)  |                                                  |
|  +--------------+-------------+                                                  |
|                 | raw quote                                                      |
|                 v                                                                |
|  +----------------------------+                                                  |
|  |     MarketDataProducer     |  Encodes tick via SBE (MarketTickEncoder)        |
|  +--------------+-------------+                                                  |
|                 | offers DirectBuffer                                            |
|                 v                                                                |
|  +----------------------------------------------------------------------------+  |
|  |                 MarketEventBus Interface                                   |  |
|  |   [ AeronEventBus (Embedded ArchivingMediaDriver) | KafkaEventBus (Future) ]|  |
|  +--------------+---------------------------------------------+---------------+  |
|                 |                                             |                  |
|                 | Aeron IPC/UDP Subscription                  | Subscription     |
|                 v                                             v                  |
|  +----------------------------+             +-------------------------------+    |
|  |       AlertEngine          |             |  Latency Metric Recorder      |    |
|  | - SBE MarketTickDecoder    |             |  (Nanosecond delta -> Grafana)|    |
|  | - Evaluates AlertRules     |             +-------------------------------+    |
|  | - Manages Cooldowns        |                                                  |
|  +--------------+-------------+                                                  |
|                 | condition met                                                  |
|                 v                                                                |
|  +----------------------------+                                                  |
|  |      TelegramNotifier      |                                                  |
|  | - Formats markdown alert   |                                                  |
|  | - Async HTTP via HttpClient|                                                  |
|  +--------------+-------------+                                                  |
+-----------------|----------------------------------------------------------------+
                  v
       [ Telegram Bot API ]
```

### 5.1 SBE Schema (`src/main/resources/market-data-schema.xml`)
Defines the `MarketTick` message:
- `templateId`: 1, `schemaId`: 1, `version`: 0
- Fields:
  - `symbol`: Fixed 16-character array (`char[16]`) — e.g., `"RELIANCE.NS"`
  - `isin`: Fixed 12-character array (`char[12]`) — e.g., `"INE002A01018"`
  - `sedol`: Fixed 7-character array (`char[7]`) — e.g., `"6714072"`
  - `ric`: Fixed 16-character array (`char[16]`) — e.g., `"RELI.NS"`
  - `price`: `uint64` (scaled integer, e.g., price $\times 10^4$ for exact decimal representation without floating-point errors)
  - `quoteTimestampNs`: `int64` (quote source/exchange time in epoch nanoseconds)
  - `ingressTimestampNs`: `int64` (epoch nanoseconds when ingestion adapter received the quote)
  - `fetchDurationNs`: `int64` (round-trip network fetch duration in nanoseconds)
  - `provider`: `enum` (`YAHOO`, `ZERODHA`, `DHAN`, `SIMULATED`)


### 5.2 Pluggable Event Bus Interface (`bus/MarketEventBus.java`)
```java
public interface MarketEventBus extends AutoCloseable {
    void start();
    boolean publish(int streamId, DirectBuffer buffer, int offset, int length);
    void subscribe(int streamId, FragmentHandler handler);
}
```
- **Stream Semantics (Builder Decision):**
  - Live pub/sub for real-time consumers (`AlertEngine`, live frontend broadcaster).
  - Background durable recording via embedded `ArchivingMediaDriver` so all tick history is persisted in the archive catalog for replay and latency metrics analysis, without causing alert storms upon application restarts.

### 5.3 Pluggable Market Data Provider Interface (`provider/MarketDataProvider.java`)
```java
public interface MarketDataProvider extends AutoCloseable {
    void start();
    void subscribe(List<String> symbols);
    void registerTickListener(Consumer<RawMarketQuote> consumer);
}
```
- **`YahooFinanceProvider` Implementation:**
  - Uses Java 25 `HttpClient` with asynchronous `sendAsync()`.
  - Periodic scheduler (default: configurable interval, 3–5 seconds) fetching snapshot quotes.
  - **Deduplication (Builder Decision):** Caches the last quote timestamp/price per symbol. When polling returns an identical timestamp/price (e.g., outside market hours or during illiquid periods), publishing is suppressed to avoid redundant processing.
  - Passes new `RawMarketQuote` to `MarketDataProducer`, which writes SBE bytes into an `UnsafeBuffer` and publishes to `MarketEventBus`.

### 5.4 Alert Engine (`alert/AlertEngine.java`)
- Consumes fragments from `MarketEventBus`.
- Decodes using `MarketTickDecoder`.
- Maintains active `AlertRule` definitions:
  - `ruleId: String`
  - `symbol: String`
  - `targetPrice: double`
  - `direction: Direction` (`ABOVE` / `BELOW`)
  - `state: AlertState` (`ARMED`, `TRIGGERED`)
  - `cooldownDuration: Duration`
- **Trigger Semantics (Builder Decision):**
  - **Edge-triggered:** The alert triggers when the price crosses the threshold (e.g. was $< 2,500$ and becomes $\ge 2,500$). Upon firing, it transitions to `TRIGGERED` and dispatches a Telegram notification. It will not fire again until the price retreats back across the threshold or the user re-arms the rule, preventing alert spam.


### 5.5 Telegram Notifier (`alert/TelegramNotifier.java`)
- Asynchronous non-blocking HTTP dispatch using Java `HttpClient`.
- Reads `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` from environment or configuration.
- Formats clean markdown alert messages with symbol, current price, condition, and timestamp.
- Resilient to network errors (logs failure without crashing the tick polling loop).

---

## 6. What's Left to the Builder
- Setup of Gradle build file (`build.gradle.kts` or `build.gradle`) with SBE code generation task.
- Directory layout matching standard Java conventions (`src/main/java`, `src/test/java`).
- Configuration mechanism (JSON or properties/env-based) for symbols and alert rules.
- JUnit 5 integration tests verifying end-to-end flow: Ingestion -> SBE encoding -> Aeron Bus publication -> Subscription -> Alert trigger.
