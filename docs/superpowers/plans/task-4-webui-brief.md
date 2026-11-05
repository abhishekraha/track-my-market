### Task 4: Modern Web Dashboard UI (TradingView Charts & Watchlist Bar)

**Files:**
- Create: `src/main/resources/web/index.html`

**Requirements:**
1. Modern, responsive financial trading interface:
   - Dark theme styling (slate dark `#0f172a`, `#1e293b`, `#334155`, text `#f8fafc`, monospace tabular numbers).
   - Top Header:
     - Logo: "TRACK MY MARKET" with a live status indicator (pulsing green dot for "CONNECTED", red for "DISCONNECTED").
     - Latency HUD chip badges: Network Ingestion Latency (ms), Aeron Bus Latency (µs), Message Counter.
   - Dynamic Watchlist Search & Add Bar:
     - Input field with placeholder "Enter stock symbol (e.g. BAJFINANCE.NS, WIPRO.NS)" with auto-uppercase.
     - "+ Add Stock" button that triggers `fetch('./api/symbols', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ symbol }) })`.
     - Instant feedback on success or invalid input.
   - Live Quote Watchlist Table:
     - Columns: Symbol, Price, Change %, ISIN, SEDOL, RIC, Network Fetch (ms), Bus Latency (µs), Actions (Remove button calling `DELETE ./api/symbols/{symbol}`).
     - Visual green flash animation on price increase, red flash animation on price decrease.
     - Clicking any row selects that stock as the active chart symbol!
   - Interactive Live Price Chart:
     - Embedded using TradingView Lightweight Charts (load script from `https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js` with fallback canvas).
     - Renders area/line chart for the currently selected stock, automatically updating with new tick prices and time axis.
   - Streaming connection:
     - Connects via `new EventSource('./api/stream')`.
     - Handles `initial-snapshot` event to populate all cached quotes immediately.
     - Handles `tick` event to update prices, flash table cells, update HUD badges, and feed the chart.
     - Automatically handles reconnection if network drops.
   - Crucial: All API endpoints must use **relative paths** (`./api/stream`, `./api/symbols`) so the dashboard works 100% identically whether hosted at `/` or behind an Nginx proxy under `/market/`.

2. Verify file exists and is well-formed HTML.
3. Commit to git and report back.
