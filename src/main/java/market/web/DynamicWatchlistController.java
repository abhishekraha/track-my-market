package market.web;

import market.provider.MarketDataProvider;
import market.provider.yahoo.YahooFinanceProvider;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * REST controller for dynamically querying, adding, and removing monitored symbols.
 */
public class DynamicWatchlistController {

    private static final Pattern SYMBOL_JSON_PATTERN =
        Pattern.compile("\"symbol\"\\s*:\\s*\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);
    private static final Pattern VALID_SYMBOL_PATTERN =
        Pattern.compile("^[A-Z0-9_.-]+$");

    private final MarketDataProvider provider;

    public DynamicWatchlistController(MarketDataProvider provider) {
        this.provider = Objects.requireNonNull(provider, "provider must not be null");
    }

    /**
     * Returns the active subscribed symbols as a JSON array string.
     * Example: ["RELIANCE.NS", "TCS.NS"]
     *
     * @return JSON array string of subscribed symbols
     */
    public String handleGetSymbolsJson() {
        List<String> symbols = provider.getSubscribedSymbols();
        return formatSymbolsJson(symbols);
    }

    /**
     * Returns a JSON map of current symbol statuses and error messages if any.
     *
     * @return JSON object string mapping symbol to status details
     */
    public String handleGetSymbolStatusJson() {
        List<String> symbols = provider.getSubscribedSymbols();
        java.util.Map<String, String> errors = java.util.Collections.emptyMap();
        market.provider.yahoo.YahooFinanceProvider yahoo = null;
        if (provider instanceof market.provider.yahoo.YahooFinanceProvider y) {
            yahoo = y;
            errors = y.getSymbolErrors();
        }

        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (String sym : symbols) {
            if (!first) {
                sb.append(",");
            }
            first = false;

            boolean isError = errors.containsKey(sym);
            String errReason = errors.get(sym);
            String companyName = (yahoo != null) ? yahoo.getCompanyName(sym) : sym;
            boolean isTradable = (yahoo != null) && !isError && yahoo.isMarketOpen(sym);
            String marketState = isError ? "DELISTED" : (isTradable ? "OPEN" : "CLOSED");
            String hoursDesc = (yahoo != null) ? yahoo.getTradingHoursDescription(sym)
                : YahooFinanceProvider.getTradingHoursDescription(sym);
            String message = isError ? (errReason != null ? errReason : "Symbol error or delisted")
                                     : (isTradable ? ("Market Open (" + hoursDesc + ")")
                                                   : ("Market Closed (" + hoursDesc + ")"));
            Double prevClose = (yahoo != null) ? yahoo.getPreviousClose(sym) : null;

            sb.append("\"").append(escapeJson(sym)).append("\":{")
              .append("\"status\":\"").append(isError ? "ERROR" : "OK").append("\",")
              .append("\"companyName\":\"").append(escapeJson(companyName)).append("\",")
              .append("\"isTradable\":").append(isTradable).append(",")
              .append("\"marketState\":\"").append(marketState).append("\",")
              .append("\"message\":\"").append(escapeJson(message)).append("\"");
            if (prevClose != null) {
                sb.append(",\"previousClose\":").append(prevClose);
            }
            sb.append("}");
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Retrieves historical chart points for the specified symbol and timeframe.
     *
     * @param symbol symbol to query
     * @param timeframe timeframe string (e.g. 1h, 1d, 1m, 1y, max)
     * @return compact JSON string with chart points
     */
    public String handleGetChartJson(String symbol, String timeframe) {
        if (symbol == null || symbol.trim().isEmpty()) {
            return "{\"error\":\"Symbol parameter is required\"}";
        }
        String cleanSymbol = normalizeSymbol(symbol);
        String cleanTf = (timeframe == null || timeframe.trim().isEmpty()) ? "1d" : timeframe.trim().toLowerCase();

        if (provider instanceof market.provider.yahoo.YahooFinanceProvider yahoo) {
            try {
                return yahoo.fetchChartHistoryJson(cleanSymbol, cleanTf).get(10, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception e) {
                return "{\"error\":\"Failed to fetch chart data: " + escapeJson(e.getMessage()) + "\"}";
            }
        }
        return "{\"error\":\"Provider does not support chart history\"}";
    }

    /**
     * Parses the symbol from the request body JSON, normalizes it, subscribes to it,
     * and returns the updated symbol list.
     *
     * @param requestBody JSON string containing {"symbol": "..."}
     * @return updated JSON array of subscribed symbols
     * @throws IllegalArgumentException if the request body is malformed or symbol is invalid
     */
    public String handleAddSymbolJson(String requestBody) {
        if (requestBody == null || requestBody.isBlank()) {
            throw new IllegalArgumentException("Request body cannot be null or empty");
        }

        Matcher matcher = SYMBOL_JSON_PATTERN.matcher(requestBody);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Malformed JSON payload: missing 'symbol' field");
        }

        String rawSymbol = matcher.group(1);
        String normalizedSymbol = normalizeSymbol(rawSymbol);

        provider.subscribe(List.of(normalizedSymbol));
        return handleGetSymbolsJson();
    }

    /**
     * Normalizes the symbol, unsubscribes from it, and returns the updated symbol list.
     *
     * @param symbol symbol to unsubscribe
     * @return updated JSON array of subscribed symbols
     * @throws IllegalArgumentException if the symbol is invalid
     */
    public String handleRemoveSymbolJson(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("Symbol must not be null or empty");
        }

        String normalizedSymbol = normalizeSymbol(symbol);
        provider.unsubscribe(normalizedSymbol);
        return handleGetSymbolsJson();
    }

    /**
     * Searches for matching stock instruments and returns a JSON array of search results.
     *
     * @param query search text
     * @return JSON array of search results
     */
    public String handleSearchJson(String query) {
        if (query == null || query.trim().isEmpty()) {
            return "[]";
        }
        if (provider instanceof market.provider.yahoo.YahooFinanceProvider yahoo) {
            try {
                var results = yahoo.search(query).get(5, java.util.concurrent.TimeUnit.SECONDS);
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < results.size(); i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    var r = results.get(i);
                    sb.append("{")
                      .append("\"symbol\":\"").append(escapeJson(r.symbol())).append("\",")
                      .append("\"name\":\"").append(escapeJson(r.name())).append("\",")
                      .append("\"exchange\":\"").append(escapeJson(r.exchange())).append("\",")
                      .append("\"type\":\"").append(escapeJson(r.type())).append("\"")
                      .append("}");
                }
                sb.append("]");
                return sb.toString();
            } catch (Exception e) {
                return "[]";
            }
        }
        return "[]";
    }

    private String normalizeSymbol(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("Symbol must not be null");
        }
        String trimmed = raw.trim().toUpperCase();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Symbol must not be empty");
        }
        if (!VALID_SYMBOL_PATTERN.matcher(trimmed).matches()) {
            throw new IllegalArgumentException("Invalid symbol: '" + raw + "'. Symbol must match pattern " + VALID_SYMBOL_PATTERN.pattern());
        }
        return trimmed;
    }

    private String formatSymbolsJson(List<String> symbols) {
        if (symbols == null || symbols.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < symbols.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("\"").append(escapeJson(symbols.get(i))).append("\"");
        }
        sb.append("]");
        return sb.toString();
    }

    private String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
