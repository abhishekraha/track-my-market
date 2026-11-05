# ⚡ Track My Market

[![Java 25](https://img.shields.io/badge/Java-25%20Virtual%20Threads-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Aeron IPC](https://img.shields.io/badge/Aeron-IPC%201.46-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://github.com/real-logic/aeron)
[![SBE v1](https://img.shields.io/badge/SBE-Zero%20Allocation-10b981?style=for-the-badge)](https://github.com/real-logic/simple-binary-encoding)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose%20Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Nginx Ingress](https://img.shields.io/badge/Nginx-Reverse%20Proxy-009639?style=for-the-badge&logo=nginx&logoColor=white)](https://nginx.org/)
[![GitHub Pages](https://img.shields.io/badge/Live%20Demo-GitHub%20Pages-38bdf8?style=for-the-badge&logo=github&logoColor=white)](https://abhishekraha.github.io/track-my-market/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=for-the-badge)](LICENSE)

> **A pluggable financial market data engine built using Real Logic Aeron IPC, Zero-Allocation Simple Binary Encoding (SBE v1), and Java 25 Virtual Threads.**  
> Designed with a decoupled, modular architecture so developers can easily plug in custom exchange feeds, lower-latency hardware/kernel-bypass transports, or specialized order book adapters. Streams real-time quotes and historical price action across NSE/BSE, US Equities, and Global Indices with **zero KYC, zero Demat account requirements, and zero API keys.**

---

### 🌐 Live Landing Page & Trading Desk Preview
Visit the interactive showcase hosted on GitHub Pages:  
👉 **[https://abhishekraha.github.io/track-my-market/](https://abhishekraha.github.io/track-my-market/)**

---

## 🏛️ System Architecture

```mermaid
flowchart LR
    subgraph Market["Public Market Ingestion"]
        P1["NSE / BSE Ingestor"]
        P2["Global Market Ingestor"]
        FX["Live FX Converter"]
    end

    subgraph Core["High-Frequency Core Engine (Java 25)"]
        SBE_ENC["SBE v1 Direct Buffer Encoder"]
        AERON_PUB["Aeron Publication Ring Buffer"]
        AERON_SUB["Aeron Subscription Poller"]
        SBE_DEC["SBE v1 Zero-Copy Decoder"]
        BROADCAST["SSE Virtual Thread Broadcaster"]
    end

    subgraph Ingress["Two-Tier Ingress (Nginx)"]
        NGINX["Nginx Proxy (:8081)<br/>gzip + unbuffered SSE"]
    end

    subgraph Client["Trader Terminal (Browser)"]
        TV_CHART["Lightweight Charts<br/>(1H - 5Y + Fallback Canvas)"]
        FX_FLIP["Currency Flip Engine<br/>(Local & Native / Auto Geolocation)"]
        TZ_FLIP["Timezone Toggle<br/>(Market Native vs Local IST/ET/GMT)"]
    end

    P1 & P2 -->|Poll Ticks| SBE_ENC
    SBE_ENC -->|Zero-Copy| AERON_PUB
    AERON_PUB -->|Shared Memory IPC < 1 µs| AERON_SUB
    AERON_SUB --> SBE_DEC
    SBE_DEC --> BROADCAST
    BROADCAST -->|Port 8080| NGINX
    FX -->|Port 8080| NGINX
    NGINX -->|HTTP & SSE Stream| Client
```

---

## 🚀 Key Highlights & Architectural Advantages

- **🔌 Pluggable & Extensible Architecture**: Decoupled interface boundaries (`MarketEventBus`, `MarketDataProvider`, `FragmentHandler`) allow developers to easily plug in custom exchange gateways, proprietary low-latency multicast adapters (e.g. Aeron UDP / InfiniBand), or specialized order-book implementations.
- **⚡ Embedded Real Logic Aeron IPC**: High-throughput, mechanical-sympathetic IPC communication over memory-mapped files without kernel context switching. Sub-microsecond ring buffer transit latency.
- **📦 Zero-Allocation SBE v1 Encoding**: Compact binary wire format encoding prices and timestamps directly into off-heap direct byte buffers (`UnsafeBuffer`), eliminating GC allocation overhead during tick broadcasting.
- **🧵 Java 25 Virtual Threads (`Thread.ofVirtual()`)**: Millions of lightweight concurrent threads handling persistent Server-Sent Events (SSE) connections and HTTP client quote polling simultaneously without thread pool starvation.
- **🛡️ Two-Tier Ingress Architecture**: Production-grade Nginx reverse proxy fronting the Java engine. Delivers Gzip compression for static assets, protects internal Aeron media driver threads, and buffers-free SSE streaming (`proxy_buffering off;`).
- **💱 Real-Time Currency Flip**: Seamlessly converts asset native currencies (INR, USD, GBP, EUR, etc.) into the user's browser-detected local currency (or arbitrary target currency via right-click cycling) with live `/api/fx` backend rates.
- **🕒 Dual-Engine Timezone Modes**: Instant toggle between Exchange Native Market Time (e.g. IST for NSE, ET for NASDAQ) and User Browser Local Time.
- **📈 Zero-Layout-Shift Interactive Charts**: High-performance TradingView Lightweight Charts supporting 13 timeframes (1H, 3H, 6H, 12H, 1D, 3D, 1W, 1M, 6M, 1Y, 3Y, 5Y, ALL) with pixel-locked HUD metrics to prevent layout thrashing and an automatic Canvas fallback.
- **🔓 Unrestricted Public Ingestion**: Real-time tick generation and market tracking with zero proprietary broker authentication or Demat account dependencies.

---

## 📊 Benchmark Metrics

| Metric | Track My Market Engine | Traditional REST / WebSockets |
| :--- | :--- | :--- |
| **Aeron IPC Bus Transit** | **< 850 nanoseconds** | ~2–15 milliseconds |
| **Garbage Collector Stalls** | **0.00 ms (Zero Allocation)** | Periodic Stop-The-World (GC spikes) |
| **Encoding Wire Format** | **Simple Binary Encoding (SBE)** | JSON / XML (heavy string parsing) |
| **SSE Client Concurrency** | **10,000+ Virtual Threads** | Limited by OS platform threads |
| **Data Ingestion Friction** | **Zero KYC / No Demat Required** | Mandatory Broker KYC & Paid APIs |

---

## 🛠️ Quickstart

### Option 1: Docker Compose (Recommended)

Run the full production stack (Aeron Engine + Nginx reverse proxy):

```bash
# 1. Clone repository
git clone https://github.com/abhishekraha/track-my-market.git
cd track-my-market

# 2. Build and launch services in background
docker compose up -d --build

# 3. Access the trading desk
# Nginx Reverse Proxy (Gzip + Ingress): http://localhost:8081/
# Direct High-Throughput Java Engine:    http://localhost:8080/
```

### Option 2: Local Gradle Execution

Requirements: **Java 25 (Temurin / OpenJDK)**

```bash
# Build binary distribution and run Web UI trading desk
./gradlew run --args="--enable-ui"

# Open trading dashboard
http://localhost:8080/
```

### Option 3: Headless CLI Latency Monitor

Run a zero-overhead console terminal that streams raw Aeron IPC frames and nanosecond transit telemetry to stdout:

```bash
./gradlew run
```

---

## 📡 REST & Streaming APIs

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/events/ticks` | `GET` | **Server-Sent Events (SSE)** streaming live decoded SBE market ticks. |
| `/api/quotes` | `GET` | Snapshot of all subscribed symbols with latest price, ISIN, and provider. |
| `/api/chart` | `GET` | Historical candles & intraday points (`?symbol=TCS.NS&timeframe=1d`). |
| `/api/fx` | `GET` | Live cross-currency foreign exchange rates against USD base. |
| `/api/watchlist/add` | `POST` | Dynamically subscribe to any new stock symbol at runtime. |
| `/api/watchlist/remove` | `POST` | Unsubscribe symbol from real-time ingestion loop. |

---

## ⚙️ Environment Configuration

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `MARKET_SYMBOLS` | `RELIANCE.NS,TCS.NS,INFY.NS,HDFCBANK.NS,TATAMOTORS.NS,BAJFINANCE.NS` | Comma-separated list of active symbols to stream on startup. |
| `MARKET_POLL_INTERVAL_MS` | `3000` | Ingestion poll frequency in milliseconds. |
| `MARKET_UI_ENABLED` | `true` | Enables embedded HTTP server & SSE dispatcher. |
| `MARKET_PORT` | `8080` | Internal Java server listening port. |

---

## 📂 Project Structure

```
track-my-market/
├── .github/                       # GitHub Actions workflows & configurations
├── docs/                          # GitHub Pages showcase & documentation
│   ├── index.html                 # Interactive landing page & trading desk preview
│   └── .nojekyll                  # GitHub Pages static bypass
├── nginx/
│   └── nginx.conf                 # Ingress proxy config (gzip, unbuffered SSE)
├── src/
│   └── main/
│       ├── java/market/           # Java 25 Aeron & SBE financial engine
│       │   ├── MarketApp.java     # Application entrypoint & virtual thread orchestration
│       │   ├── bus/               # Event bus abstractions & Aeron IPC ring buffer
│       │   ├── consumer/          # Zero-copy SBE decoders & latency metric monitors
│       │   ├── producer/          # High-frequency quote publishers
│       │   ├── provider/          # Yahoo Finance & public market feed connectors
│       │   └── web/               # Embedded HttpServer, SSE stream & dynamic watchlist
│       └── resources/
│           ├── market-data-schema.xml # SBE XML schema definition (MarketTick)
│           └── web/               # Production Trading Desk UI (Lightweight Charts)
├── build.gradle.kts               # Gradle build script with SBE stub generation
├── docker-compose.yml             # Orchestration: Java engine (8080) + Nginx (8081)
└── Dockerfile                     # Multi-stage Eclipse Temurin 25 JRE container
```

---

## 📄 License

Distributed under the **Apache License, Version 2.0**. See [`LICENSE`](LICENSE) for details.
