### Task 3: Embedded Web Server & Context Path Routing

**Files:**
- Create: `src/main/java/market/web/MarketHttpServer.java`
- Test: `src/test/java/market/web/MarketHttpServerTest.java`

**Requirements:**
1. Implement `MarketHttpServer`:
   - Constructor: `public MarketHttpServer(String bindHost, int port, String contextPath, MarketStreamBroadcaster broadcaster, DynamicWatchlistController watchlistController)`
   - Configures `HttpServer.create(new InetSocketAddress(bindHost, port), 0)`.
   - Normalizes `contextPath`: ensures leading slash, trims trailing slash (unless root `/`). e.g. `""` -> `""`, `"/market/"` -> `"/market"`.
   - Supports:
     - `GET {contextPath}/` and `GET {contextPath}/index.html`: serves static `index.html` from classpath `/web/index.html` (or fallback HTML) with `Content-Type: text/html; charset=UTF-8`.
     - `GET {contextPath}/api/stream`: Server-Sent Events endpoint:
       - Sends response headers: `200`, `Content-Type: text/event-stream; charset=UTF-8`, `Cache-Control: no-cache, no-transform`, `Connection: keep-alive`, `X-Accel-Buffering: no` (crucial for Nginx SSE pass-through).
       - Immediately sends `event: initial-snapshot\ndata: <broadcaster.getInitialSnapshotJson()>\n\n`.
       - Registers client stream with `broadcaster.registerClient(os)`.
     - `GET {contextPath}/api/symbols`: calls `watchlistController.handleGetSymbolsJson()` returning `200` with `application/json`.
     - `POST {contextPath}/api/symbols`: reads body, calls `watchlistController.handleAddSymbolJson(body)` returning `200` with `application/json` (or `400` on `IllegalArgumentException`).
     - `DELETE {contextPath}/api/symbols/{symbol}`: parses symbol path, calls `watchlistController.handleRemoveSymbolJson(symbol)` returning `200` with `application/json`.
   - `start()`, `stop()`, `getPort()`, `getContextPath()`.

2. Create `src/test/java/market/web/MarketHttpServerTest.java`:
   - Start server on port `0` (ephemeral port) with context path `/market` and test all routes with Java's `HttpClient`.
   - Test static HTML serving.
   - Test GET /api/symbols, POST /api/symbols, DELETE /api/symbols.
   - Test GET /api/stream headers and initial snapshot.
   - Verify `.\gradlew.bat test --tests market.web.MarketHttpServerTest` passes.

3. Commit your work to git when complete and report back.
