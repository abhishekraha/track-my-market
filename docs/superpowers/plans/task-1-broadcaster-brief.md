### Task 1: Aeron-to-SSE Broadcaster Bridge & Quote Cache

**Files:**
- Create: `src/main/java/market/web/MarketStreamBroadcaster.java`
- Test: `src/test/java/market/web/MarketStreamBroadcasterTest.java`

**Requirements:**
1. Implement `MarketStreamBroadcaster`:
   - Implements `io.aeron.logbuffer.FragmentHandler`.
   - Thread-safe client connection registry:
     - `void registerClient(OutputStream clientOut)`
     - `void unregisterClient(OutputStream clientOut)`
     - `int getActiveClientCount()`
   - Quote caching:
     - Keeps an in-memory concurrent map of the latest `MarketTickView` or quote for each symbol.
     - `String getInitialSnapshotJson()` returns a JSON array of all latest quotes for newly connected SSE clients.
   - On fragment:
     - Check header `templateId == MarketTickDecoder.TEMPLATE_ID`.
     - Decode `MarketTickDecoder`: extract `symbol`, `price` (divide by 10000.0), `isin`, `sedol`, `ric`, `quoteTimestampNs`, `ingressTimestampNs`, `fetchDurationNs`, `provider`.
     - Compute `busLatencyUs = Math.max(0, (nowEpochNs - ingressTimestampNs) / 1000.0)`.
     - Construct SSE event:
       ```
       event: tick
       data: {"symbol":"...","price":...,"isin":"...","sedol":"...","ric":"...","quoteTime":...,"fetchDurationMs":...,"busLatencyUs":...,"provider":"..."}

       ```
     - Flush to all registered clients. If an `OutputStream` throws an `IOException` (client closed connection), automatically remove it from the active clients list without crashing the fragment loop.

2. Create `src/test/java/market/web/MarketStreamBroadcasterTest.java`:
   - Test encoding a `MarketTick` into an `UnsafeBuffer` and passing it to `onFragment`.
   - Assert that registered client received `event: tick\n` with JSON fields.
   - Assert `getInitialSnapshotJson()` contains the cached quote.
   - Verify `.\gradlew.bat test --tests market.web.MarketStreamBroadcasterTest` passes.

3. Commit your work to git when complete and report back.
