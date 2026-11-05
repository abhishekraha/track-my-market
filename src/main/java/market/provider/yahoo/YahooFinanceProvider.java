package market.provider.yahoo;

import market.provider.MarketDataProvider;
import market.provider.RawMarketQuote;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Yahoo Finance polling market data ingestion adapter.
 * <p>
 * Connects directly or through an Nginx reverse proxy with configurable base URL,
 * maps institutional identifiers (ISIN, SEDOL, RIC), deduplicates redundant quotes,
 * and emits parsed quotes to registered listeners.
 */
public class YahooFinanceProvider implements MarketDataProvider {

    public static final String DEFAULT_BASE_URL = "https://query1.finance.yahoo.com";
    public static final String SYSTEM_PROPERTY_BASE_URL = "market.provider.yahoo.base-url";
    public static final String ENV_VAR_BASE_URL = "YAHOO_FINANCE_BASE_URL";
    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(5);

    private static final Pattern REGULAR_MARKET_PRICE_PATTERN =
        Pattern.compile("\"regularMarketPrice\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)");
    private static final Pattern REGULAR_MARKET_TIME_PATTERN =
        Pattern.compile("\"regularMarketTime\"\\s*:\\s*([0-9]+)");

    public record SymbolIdentifiers(String isin, String sedol, String ric) {}
    public record SearchResult(String symbol, String name, String exchange, String type) {}

    private static final Pattern QUOTE_OBJECT_PATTERN =
        Pattern.compile("\\{[^{}]*\"symbol\"\\s*:\\s*\"[^\"]+\"[^{}]*\\}");
    private static final Pattern SYMBOL_PATTERN = Pattern.compile("\"symbol\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern LONGNAME_PATTERN = Pattern.compile("\"longname\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern SHORTNAME_PATTERN = Pattern.compile("\"shortname\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern EXCH_DISP_PATTERN = Pattern.compile("\"exchDisp\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern EXCHANGE_PATTERN = Pattern.compile("\"exchange\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern QUOTE_TYPE_PATTERN = Pattern.compile("\"quoteType\"\\s*:\\s*\"([^\"]+)\"");

    private final String baseUrl;
    private final Duration pollInterval;
    private final boolean ownsHttpClient;
    private final boolean ownsScheduler;

    private HttpClient httpClient;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> pollingTask;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Set<String> subscribedSymbols = ConcurrentHashMap.newKeySet();
    private final List<Consumer<RawMarketQuote>> listeners = new CopyOnWriteArrayList<>();

    private final Map<String, Long> lastQuoteTimes = new ConcurrentHashMap<>();
    private final Map<String, Double> lastPrices = new ConcurrentHashMap<>();
    private final Map<String, SymbolIdentifiers> identifierMap = new ConcurrentHashMap<>();

    public YahooFinanceProvider() {
        this(null, null, null, null);
    }

    public YahooFinanceProvider(String baseUrl) {
        this(baseUrl, null, null, null);
    }

    public YahooFinanceProvider(String baseUrl, HttpClient httpClient) {
        this(baseUrl, httpClient, null, null);
    }

    public YahooFinanceProvider(String baseUrl, HttpClient httpClient, ScheduledExecutorService scheduler, Duration pollInterval) {
        this.baseUrl = resolveBaseUrl(baseUrl);
        this.pollInterval = pollInterval != null ? pollInterval : resolvePollInterval();

        this.ownsHttpClient = (httpClient == null);
        this.httpClient = httpClient;

        this.ownsScheduler = (scheduler == null);
        this.scheduler = scheduler;

        initDefaultIdentifierMappings();
    }

    private static String resolveBaseUrl(String explicit) {
        if (explicit != null && !explicit.isBlank()) {
            return normalizeUrl(explicit);
        }
        String sysProp = System.getProperty(SYSTEM_PROPERTY_BASE_URL);
        if (sysProp != null && !sysProp.isBlank()) {
            return normalizeUrl(sysProp);
        }
        String envVal = System.getenv(ENV_VAR_BASE_URL);
        if (envVal != null && !envVal.isBlank()) {
            return normalizeUrl(envVal);
        }
        return DEFAULT_BASE_URL;
    }

    private static String normalizeUrl(String url) {
        String trimmed = url.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static Duration resolvePollInterval() {
        String prop = System.getProperty("market.provider.yahoo.poll-interval-ms");
        if (prop != null && !prop.isBlank()) {
            try {
                return Duration.ofMillis(Long.parseLong(prop.trim()));
            } catch (NumberFormatException ignored) {
            }
        }
        return DEFAULT_POLL_INTERVAL;
    }

    private void initDefaultIdentifierMappings() {
        registerIdentifierMapping("RELIANCE.NS", "INE002A01018", "6714072", "RELI.NS");
        registerIdentifierMapping("TCS.NS", "INE467B01029", "6873587", "TCS.NS");
        registerIdentifierMapping("INFY.NS", "INE009A01021", "6459397", "INFY.NS");
    }

    public void registerIdentifierMapping(String symbol, String isin, String sedol, String ric) {
        if (symbol != null) {
            identifierMap.put(symbol, new SymbolIdentifiers(isin, sedol, ric));
        }
    }

    public SymbolIdentifiers getIdentifiers(String symbol) {
        if (symbol == null) {
            return new SymbolIdentifiers("UNKNOWN", "UNKNOWN", "UNKNOWN");
        }
        return identifierMap.computeIfAbsent(symbol, s ->
            new SymbolIdentifiers("UNKNOWN", "UNKNOWN", s)
        );
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public boolean isRunning() {
        return running.get();
    }

    @Override
    public List<String> getSubscribedSymbols() {
        return subscribedSymbols.stream().sorted().toList();
    }

    @Override
    public void subscribe(List<String> symbols) {
        if (symbols != null) {
            for (String s : symbols) {
                if (s != null && !s.isBlank()) {
                    String trimmed = s.trim();
                    boolean added = subscribedSymbols.add(trimmed);
                    if (added && running.get()) {
                        pollSymbol(trimmed);
                    }
                }
            }
        }
    }

    @Override
    public void unsubscribe(String symbol) {
        if (symbol != null && !symbol.isBlank()) {
            String trimmed = symbol.trim();
            subscribedSymbols.remove(trimmed);
            lastQuoteTimes.remove(trimmed);
            lastPrices.remove(trimmed);
        }
    }

    @Override
    public void registerTickListener(Consumer<RawMarketQuote> listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    @Override
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        if (httpClient == null) {
            httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        }

        if (scheduler == null) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "yahoo-finance-poller");
                thread.setDaemon(true);
                return thread;
            });
        }

        pollingTask = scheduler.scheduleWithFixedDelay(
            this::pollAllSymbols,
            0,
            pollInterval.toMillis(),
            TimeUnit.MILLISECONDS
        );
    }

    @Override
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        if (pollingTask != null) {
            pollingTask.cancel(true);
            pollingTask = null;
        }

        if (ownsScheduler && scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }

        if (ownsHttpClient && httpClient != null) {
            try {
                httpClient.close();
            } catch (Exception ignored) {
            }
            httpClient = null;
        }
    }

    public void pollAllSymbols() {
        if (!running.get()) {
            return;
        }
        for (String symbol : subscribedSymbols) {
            pollSymbol(symbol);
        }
    }

    public CompletableFuture<Void> pollSymbol(String symbol) {
        if (httpClient == null) {
            return CompletableFuture.completedFuture(null);
        }

        String uri = buildSymbolUri(symbol);
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(uri))
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) track-my-market/1.0")
            .header("Accept", "application/json")
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();

        long startNs = System.nanoTime();
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenAccept(response -> {
                long durationNs = System.nanoTime() - startNs;
                if (!running.get()) {
                    return;
                }
                if (response.statusCode() == 200) {
                    parseAndEmit(symbol, response.body(), durationNs);
                }
            })
            .exceptionally(ex -> null);
    }

    public String buildSymbolUri(String symbol) {
        String encoded = URLEncoder.encode(symbol, StandardCharsets.UTF_8);
        if (baseUrl.endsWith("/v8/finance/chart")) {
            return baseUrl + "/" + encoded;
        }
        return baseUrl + "/v8/finance/chart/" + encoded;
    }

    /**
     * Parses regularMarketPrice and regularMarketTime from Yahoo Finance chart JSON response
     * and emits a {@link RawMarketQuote} to registered listeners unless suppressed by deduplication.
     *
     * @param symbol ticker symbol
     * @param jsonResponse response payload from Yahoo Finance
     * @param fetchDurationNs round-trip network duration in nanoseconds
     */
    public void parseAndEmit(String symbol, String jsonResponse, long fetchDurationNs) {
        if (symbol == null || jsonResponse == null || jsonResponse.isBlank()) {
            return;
        }

        Matcher priceMatcher = REGULAR_MARKET_PRICE_PATTERN.matcher(jsonResponse);
        Matcher timeMatcher = REGULAR_MARKET_TIME_PATTERN.matcher(jsonResponse);

        if (!priceMatcher.find() || !timeMatcher.find()) {
            return;
        }

        double price;
        long rawTime;
        try {
            price = Double.parseDouble(priceMatcher.group(1));
            rawTime = Long.parseLong(timeMatcher.group(1));
        } catch (NumberFormatException e) {
            return;
        }

        // Convert seconds to epoch nanoseconds (Yahoo Finance returns epoch seconds ~10 digits)
        long quoteTimestampEpochNs;
        if (rawTime < 100_000_000_000L) {
            quoteTimestampEpochNs = rawTime * 1_000_000_000L;
        } else if (rawTime < 100_000_000_000_000L) {
            quoteTimestampEpochNs = rawTime * 1_000_000L;
        } else {
            quoteTimestampEpochNs = rawTime;
        }

        // Deduplication: suppress duplicate ticks with unchanged timestamp and price
        Long lastTime = lastQuoteTimes.get(symbol);
        Double lastPrice = lastPrices.get(symbol);
        if (lastTime != null && lastTime == quoteTimestampEpochNs
                && lastPrice != null && Double.compare(lastPrice, price) == 0) {
            return;
        }

        lastQuoteTimes.put(symbol, quoteTimestampEpochNs);
        lastPrices.put(symbol, price);

        SymbolIdentifiers ids = getIdentifiers(symbol);
        RawMarketQuote quote = new RawMarketQuote(
            symbol,
            price,
            quoteTimestampEpochNs,
            fetchDurationNs,
            ids.isin(),
            ids.sedol(),
            ids.ric()
        );

        for (Consumer<RawMarketQuote> listener : listeners) {
            try {
                listener.accept(quote);
            } catch (Throwable ignored) {
            }
        }
    }

    public void clearDeduplicationCache() {
        lastQuoteTimes.clear();
        lastPrices.clear();
    }

    public Long getLastQuoteTime(String symbol) {
        return lastQuoteTimes.get(symbol);
    }

    public Double getLastPrice(String symbol) {
        return lastPrices.get(symbol);
    }

    public List<SearchResult> parseSearchResults(String json) {
        if (json == null || json.isBlank()) {
            return Collections.emptyList();
        }
        List<SearchResult> results = new ArrayList<>();
        Matcher matcher = QUOTE_OBJECT_PATTERN.matcher(json);
        while (matcher.find()) {
            String quoteBlock = matcher.group();
            Matcher symM = SYMBOL_PATTERN.matcher(quoteBlock);
            if (!symM.find()) {
                continue;
            }
            String symbol = symM.group(1);

            String name = symbol;
            Matcher longM = LONGNAME_PATTERN.matcher(quoteBlock);
            if (longM.find()) {
                name = longM.group(1);
            } else {
                Matcher shortM = SHORTNAME_PATTERN.matcher(quoteBlock);
                if (shortM.find()) {
                    name = shortM.group(1);
                }
            }

            String exchange = "NSE";
            Matcher exchDispM = EXCH_DISP_PATTERN.matcher(quoteBlock);
            if (exchDispM.find()) {
                exchange = exchDispM.group(1);
            } else {
                Matcher exchM = EXCHANGE_PATTERN.matcher(quoteBlock);
                if (exchM.find()) {
                    exchange = exchM.group(1);
                }
            }

            String type = "EQUITY";
            Matcher typeM = QUOTE_TYPE_PATTERN.matcher(quoteBlock);
            if (typeM.find()) {
                type = typeM.group(1);
            }

            results.add(new SearchResult(symbol, name, exchange, type));
        }
        return results;
    }

    public CompletableFuture<List<SearchResult>> search(String query) {
        if (query == null || query.trim().isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        String cleanQuery = query.trim();
        String encodedQuery = URLEncoder.encode(cleanQuery, StandardCharsets.UTF_8);
        String searchUrl = baseUrl + "/v1/finance/search?q=" + encodedQuery + "&quotesCount=8&newsCount=0";

        HttpClient client = (this.httpClient != null) ? this.httpClient : HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(searchUrl))
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) track-my-market/1.0")
            .header("Accept", "application/json")
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply(response -> {
                if (response.statusCode() == 200) {
                    return parseSearchResults(response.body());
                }
                return Collections.<SearchResult>emptyList();
            })
            .exceptionally(ex -> Collections.emptyList());
    }
}
