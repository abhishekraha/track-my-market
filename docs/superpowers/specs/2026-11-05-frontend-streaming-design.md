# Design Doc: Frontend Streaming Server & Real-Time Trading Dashboard

**Date:** 2026-11-05  
**Topic:** track-my-market (Subsystem 2: Frontend Streaming & Interactive Dashboard)  
**Status:** In Review  

---

## 1. Intent & The Why
The goal of this subsystem is to provide an interactive, real-time web dashboard for `track-my-market` that:
1. Streams live quotes directly from the Aeron message bus to web browsers with sub-millisecond dispatch latency.
2. Displays live prices, green/red flash animations on upticks/downticks, institutional identifiers (`ISIN`, `SEDOL`, `RIC`), and latency metrics (network fetch duration vs. Aeron bus propagation).
3. Allows users to dynamically add and remove stock symbols to monitor in real time without restarting the application.
4. Renders live interactive price charts using TradingView's Lightweight Charts.
5. Runs completely self-contained within the Java process (zero Node.js/npm dependencies) and supports hosting behind an Nginx reverse proxy under any custom context path.

---

## 2. Goals, Non-Goals & Anti-Goals

### Goals
- **Decoupled & Optional Module:** The UI and streaming web server reside in an isolated module (`market.web`) that is **completely optional and disabled by default**. It is activated only when `--enable-ui` or `market.ui.enabled=true` / `MARKET_UI_ENABLED=true` is supplied. When disabled, the application runs headless with zero port binding and zero web memory overhead.
- **Zero-Dependency Embedded Server:** Uses Java's embedded HTTP server (`com.sun.net.httpserver.HttpServer`) to serve static assets and handle Server-Sent Events (SSE) streaming (`text/event-stream`).
- **Dynamic Watchlist Management:** REST endpoints (`GET /api/symbols`, `POST /api/symbols`, `DELETE /api/symbols/{symbol}`) to inspect, add, and remove monitored symbols on the fly, immediately updating `YahooFinanceProvider`.
- **Low-Latency Broadcast Bridge:** An Aeron subscription component (`MarketStreamBroadcaster`) that decodes SBE ticks from the IPC bus and broadcasts JSON events to connected browser streams.
- **Modern Dark Financial UI:** Clean, dark-mode terminal dashboard with:
  - Top header with system latency gauges (Network RTT and Aeron Bus Latency in microseconds).
  - Watchlist search/add bar with instant feedback.
  - Real-time stock ticker table with flash animations.
  - High-performance interactive price chart powered by TradingView Lightweight Charts.
- **Nginx Reverse-Proxy Ready:** Configurable bind host, port, and base context path (e.g. `/` or `/market/`) with relative asset URLs and CORS/forwarded header awareness.

### Non-Goals
- Multi-user authentication, roles, or database-backed user profiles.
- Historical chart storage spanning multiple years (charts stream current session ticks with recent historical data from Yahoo).
- Order execution or portfolio tracking.

### Anti-Goals
- Requiring Node.js, npm, or complex frontend build pipelines to run the application.
- Polling HTTP from the browser instead of continuous event-driven push.

---

## 3. Constraints
- **Self-Contained Deployment:** Everything packaged in the single Gradle build and runnable via `./gradlew run`.
- **Proxy Compatibility:** Must function behind Nginx with `proxy_pass` and `proxy_buffering off` for SSE.

---

## 4. Architecture & Component Details

```
+------------------------------------------------------------------------------------+
|                                 track-my-market                                    |
|                                                                                    |
|  +--------------------+                                                            |
|  |   AeronEventBus    |                                                            |
|  +---------+----------+                                                            |
|            | SBE MarketTick (IPC)                                                  |
|            v                                                                       |
|  +--------------------------------+                                                |
|  |     MarketStreamBroadcaster    |  (Aeron FragmentHandler)                       |
|  | - Decodes SBE MarketTick       |                                                |
|  | - Computes busLatencyNs        |                                                |
|  | - Encodes JSON tick event      |                                                |
|  +---------+----------------------+                                                |
|            | pushes to active SSE client streams                                   |
|            v                                                                       |
|  +--------------------------------+       +-------------------------------------+  |
|  |      MarketHttpServer          |       |     Dynamic Watchlist Controller    |  |
|  | - GET /api/stream (SSE)        |<----->| - GET  /api/symbols                 |  |
|  | - Static Assets (index.html)   |       | - POST /api/symbols {symbol}        |  |
|  +---------+----------------------+       | - DELETE /api/symbols/{symbol}      |  |
|            |                              +------------------+------------------+  |
|            | HTTP / SSE                                      | updates watchlist   |
+------------|-------------------------------------------------|---------------------+
             |                                                 v
             v                                     [ YahooFinanceProvider ]
   [ Browser Client / Nginx Proxy ]
   - EventSource('/api/stream')
   - TradingView Lightweight Charts
   - Watchlist Add/Remove UI
```

### 4.1 Server Endpoints (`market.web.MarketHttpServer`)
- `GET /` & `GET /index.html`: Serves the embedded single-page dashboard.
- `GET /api/stream`: Server-Sent Events stream (`text/event-stream`).
  - **Initial Snapshot (Builder Decision):** Upon connection handshake, immediately sends an `initial-snapshot` event containing the latest cached quotes and latency stats for all currently monitored symbols so the UI renders immediately without waiting for the next tick.
  - Followed by continuous live `tick` events:
    ```json
    event: tick
    data: {"symbol":"RELIANCE.NS","price":2510.75,"isin":"INE002A01018","sedol":"6714072","ric":"RELI.NS","quoteTime":1730790000000,"fetchDurationMs":12.5,"busLatencyUs":4.2,"provider":"YAHOO"}
    ```
- `GET /api/symbols`: Returns JSON array of currently subscribed symbols: `["RELIANCE.NS", "TCS.NS", ...]`.
- `POST /api/symbols`: Adds a new symbol (e.g. `{"symbol": "BAJFINANCE.NS"}`).
  - **Validation & Dynamic Trigger (Builder Decision):** Performs immediate syntactic normalization (trimming, uppercase), adds the symbol to `YahooFinanceProvider`, triggers an immediate background poll, and returns HTTP 200 with the updated symbol list.
- `DELETE /api/symbols/{symbol}`: Removes a symbol from active monitoring.

### 4.2 Broadcaster Bridge (`market.web.MarketStreamBroadcaster`)
- Maintains a thread-safe set of connected SSE `HttpExchange` output streams (`CopyOnWriteArrayList<OutputStream>`).
- Caches the latest `MarketTickView` per symbol in a concurrent map to supply initial snapshots to new clients.
- Subscribes to the Aeron bus stream (`DEFAULT_STREAM_ID = 1001`).
- On every tick fragment, calculates `busLatencyUs = (nowNs - ingressNs) / 1000.0`.
- Serializes a compact JSON payload and flushes it across all connected clients. Dead client connections (e.g., closed browser tabs) are automatically detected and pruned.

### 4.3 Web Dashboard UI (`src/main/resources/web/index.html`)
- Built with standard HTML5, CSS Grid/Flexbox, and vanilla JavaScript (ES6).
- **TradingView Lightweight Charts:** Embedded via CDN script tag with a local offline fallback.
  - **Live Session Streaming (Builder Decision):** The chart visualizes live session ticks with high-precision time resolution, updating smoothly as new ticks arrive over the SSE stream. Switching stocks displays the active symbol's session tick series.
- **Dynamic Watchlist Input:** Clean search bar with auto-uppercase and "+ Add Stock" button.
- **Latency HUD:** Live updating badges displaying current Network Ingestion Latency and Aeron Bus Latency.


---

## 5. What's Left to the Builder
- Selection of HTTP server port configuration (default: `8080`, overridable via `market.server.port` / `MARKET_SERVER_PORT`).
- Context path configuration (default: `/`, overridable via `market.server.context-path` / `MARKET_SERVER_CONTEXT_PATH` for Nginx sub-path proxies).
- Unit tests for SSE broadcaster, watchlist REST endpoints, and integration tests with the Aeron bus.
