### Task 2: Dynamic Watchlist Controller & Symbol Subscription API

**Files:**
- Modify: `src/main/java/market/provider/MarketDataProvider.java`
- Modify: `src/main/java/market/provider/yahoo/YahooFinanceProvider.java`
- Create: `src/main/java/market/web/DynamicWatchlistController.java`
- Test: `src/test/java/market/web/DynamicWatchlistControllerTest.java`

**Requirements:**
1. In `src/main/java/market/provider/MarketDataProvider.java`:
   - Add default method: `List<String> getSubscribedSymbols()` returning the list of active symbols.
   - Add default method: `void unsubscribe(String symbol)` to remove a symbol.

2. In `src/main/java/market/provider/yahoo/YahooFinanceProvider.java`:
   - Ensure `subscribedSymbols` is thread-safe and allows dynamic additions (`subscribe(List<String>)`) and removals (`unsubscribe(String)`).
   - Implement `getSubscribedSymbols()`.
   - When a new symbol is subscribed while running, ensure it triggers an immediate poll or gets picked up in the polling loop.

3. Create `src/main/java/market/web/DynamicWatchlistController.java`:
   - Accepts `MarketDataProvider` in constructor.
   - `String handleGetSymbolsJson()`: returns JSON array e.g. `["RELIANCE.NS","TCS.NS"]`.
   - `String handleAddSymbolJson(String requestBody)`: parses `{"symbol": "..."}`, validates (non-empty, alphanumeric with `.`/`-`, trims and converts to uppercase), calls `provider.subscribe(List.of(normalizedSymbol))`, and returns JSON response with updated symbol list.
   - `String handleRemoveSymbolJson(String symbol)`: normalizes symbol, calls `provider.unsubscribe(symbol)`, and returns updated symbol list.
   - Handles malformed JSON and invalid symbols with clean error messages or HTTP error status indication.

4. Create `src/test/java/market/web/DynamicWatchlistControllerTest.java`:
   - Test retrieving symbols list as JSON.
   - Test adding valid symbol updates provider.
   - Test removing symbol updates provider.
   - Verify `.\gradlew.bat test --tests market.web.DynamicWatchlistControllerTest` passes.

5. Commit your work to git when complete and report back.
