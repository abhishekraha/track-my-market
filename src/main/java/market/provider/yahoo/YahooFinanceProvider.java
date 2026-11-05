package market.provider.yahoo;

import market.provider.MarketDataProvider;
import market.provider.RawMarketQuote;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
    public record ChartPoint(long time, double value, Double open, Double high, Double low, Double close, Long volume) {}
    public record TimeframeSpec(String range, String interval, long maxAgeSeconds) {}

    @FunctionalInterface
    public interface SymbolErrorListener {
        void onSymbolError(String symbol, String errorReason);
    }

    private static final Pattern TIMESTAMPS_PATTERN =
        Pattern.compile("\"timestamp\"\\s*:\\s*\\[([^\\]]*)\\]");
    private static final Pattern CLOSE_ARRAY_PATTERN =
        Pattern.compile("\"close\"\\s*:\\s*\\[([^\\]]*)\\]");
    private static final Pattern OPEN_ARRAY_PATTERN =
        Pattern.compile("\"open\"\\s*:\\s*\\[([^\\]]*)\\]");
    private static final Pattern HIGH_ARRAY_PATTERN =
        Pattern.compile("\"high\"\\s*:\\s*\\[([^\\]]*)\\]");
    private static final Pattern LOW_ARRAY_PATTERN =
        Pattern.compile("\"low\"\\s*:\\s*\\[([^\\]]*)\\]");
    private static final Pattern VOLUME_ARRAY_PATTERN =
        Pattern.compile("\"volume\"\\s*:\\s*\\[([^\\]]*)\\]");
    private static final Pattern PREV_CLOSE_PATTERN =
        Pattern.compile("\"(?:chartPreviousClose|previousClose)\"\\s*:\\s*([0-9]+(?:\\.[0-9]+)?)");
    private static final Pattern CURRENCY_PATTERN =
        Pattern.compile("\"currency\"\\s*:\\s*\"([^\"]+)\"");

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
    private final List<SymbolErrorListener> errorListeners = new CopyOnWriteArrayList<>();
    private final Map<String, String> symbolErrors = new ConcurrentHashMap<>();

    private final Map<String, Long> lastQuoteTimes = new ConcurrentHashMap<>();
    private final Map<String, Double> lastPrices = new ConcurrentHashMap<>();
    private final Map<String, SymbolIdentifiers> identifierMap = new ConcurrentHashMap<>();
    private final Map<String, String> companyNames = new ConcurrentHashMap<>();

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
        initDefaultCompanyNames();
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
        registerIdentifierMapping("TMPV.NS", "INE155A01022", "BP0VJ24", "TMPV.NS");
        registerIdentifierMapping("TMCV.NS", "INE155A01022", "BP0VJ24", "TMCV.NS");
    }

    private void initDefaultCompanyNames() {
        companyNames.put("RELIANCE.NS", "Reliance Industries Limited");
        companyNames.put("TCS.NS", "Tata Consultancy Services Limited");
        companyNames.put("INFY.NS", "Infosys Limited");
        companyNames.put("HDFCBANK.NS", "HDFC Bank Limited");
        companyNames.put("BAJFINANCE.NS", "Bajaj Finance Limited");
        companyNames.put("TMPV.NS", "Tata Motors Passenger Vehicles Limited");
        companyNames.put("TMCV.NS", "Tata Motors Limited (Commercial Vehicles)");
        companyNames.put("TATAMOTORS.NS", "Tata Motors Limited (Demerged legacy ticker)");
    }

    public void registerCompanyName(String symbol, String name) {
        if (symbol != null && name != null && !name.isBlank()) {
            companyNames.put(symbol.trim().toUpperCase(), name.trim());
        }
    }

    public String getCompanyName(String symbol) {
        if (symbol == null) {
            return "";
        }
        String clean = symbol.trim().toUpperCase();
        return companyNames.getOrDefault(clean, symbol);
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
            symbolErrors.remove(trimmed);
        }
    }

    @Override
    public void registerTickListener(Consumer<RawMarketQuote> listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    public void registerErrorListener(SymbolErrorListener listener) {
        if (listener != null) {
            errorListeners.add(listener);
        }
    }

    public Map<String, String> getSymbolErrors() {
        return Collections.unmodifiableMap(symbolErrors);
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
                    boolean parsed = parseAndEmit(symbol, response.body(), durationNs);
                    if (!parsed && response.body() != null && response.body().contains("\"error\":") && !response.body().contains("\"error\":null")) {
                        handleSymbolErrorResponse(symbol, response.body());
                    }
                } else {
                    handleSymbolErrorResponse(symbol, response.body());
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

    public void handleSymbolErrorResponse(String symbol, String body) {
        String reason = "No data found";
        if (body != null && body.contains("delisted")) {
            reason = "Symbol may be delisted";
        }
        if (symbol != null && (symbol.equalsIgnoreCase("TATAMOTORS.NS") || symbol.equalsIgnoreCase("TATAMOTORS"))) {
            reason = "Delisted: Tata Motors demerged into TMPV.NS (Passenger) and TMCV.NS (Commercial)";
        }
        symbolErrors.put(symbol, reason);
        for (SymbolErrorListener listener : errorListeners) {
            try {
                listener.onSymbolError(symbol, reason);
            } catch (Throwable ignored) {
            }
        }
    }

    /**
     * Parses regularMarketPrice and regularMarketTime from Yahoo Finance chart JSON response
     * and emits a {@link RawMarketQuote} to registered listeners unless suppressed by deduplication.
     *
     * @param symbol ticker symbol
     * @param jsonResponse response payload from Yahoo Finance
     * @param fetchDurationNs round-trip network duration in nanoseconds
     * @return true if tick was successfully parsed and processed
     */
    public boolean parseAndEmit(String symbol, String jsonResponse, long fetchDurationNs) {
        if (symbol == null || jsonResponse == null || jsonResponse.isBlank()) {
            return false;
        }

        Matcher priceMatcher = REGULAR_MARKET_PRICE_PATTERN.matcher(jsonResponse);
        Matcher timeMatcher = REGULAR_MARKET_TIME_PATTERN.matcher(jsonResponse);

        if (!priceMatcher.find() || !timeMatcher.find()) {
            return false;
        }

        double price;
        long rawTime;
        try {
            price = Double.parseDouble(priceMatcher.group(1));
            rawTime = Long.parseLong(timeMatcher.group(1));
        } catch (NumberFormatException e) {
            return false;
        }

        symbolErrors.remove(symbol);

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
            return true;
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
        return true;
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

            registerCompanyName(symbol, name);
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
                List<SearchResult> results = new ArrayList<>();
                if (response.statusCode() == 200) {
                    results.addAll(parseSearchResults(response.body()));
                }
                String upper = cleanQuery.toUpperCase();
                if (upper.contains("TATAMOTOR") || upper.equals("TATA")) {
                    if (results.stream().noneMatch(r -> r.symbol().equalsIgnoreCase("TMPV.NS"))) {
                        results.add(new SearchResult("TMPV.NS", "Tata Motors Passenger Vehicles Limited", "NSE", "EQUITY"));
                    }
                    if (results.stream().noneMatch(r -> r.symbol().equalsIgnoreCase("TMCV.NS"))) {
                        results.add(new SearchResult("TMCV.NS", "Tata Motors Limited (Commercial Vehicles)", "NSE", "EQUITY"));
                    }
                }
                return results;
            })
            .exceptionally(ex -> Collections.emptyList());
    }

    public static TimeframeSpec resolveTimeframeSpec(String tf) {
        if (tf == null) {
            return new TimeframeSpec("1d", "1m", 0);
        }
        return switch (tf.trim().toLowerCase()) {
            case "1h" -> new TimeframeSpec("1d", "1m", 3600);
            case "3h" -> new TimeframeSpec("1d", "1m", 10800);
            case "6h" -> new TimeframeSpec("1d", "1m", 21600);
            case "12h" -> new TimeframeSpec("1d", "1m", 43200);
            case "1d" -> new TimeframeSpec("1d", "1m", 0);
            case "3d" -> new TimeframeSpec("5d", "5m", 3 * 86400);
            case "1w" -> new TimeframeSpec("5d", "15m", 0);
            case "1m" -> new TimeframeSpec("1mo", "1d", 0);
            case "6m" -> new TimeframeSpec("6mo", "1d", 0);
            case "1y" -> new TimeframeSpec("1y", "1d", 0);
            case "3y" -> new TimeframeSpec("5y", "1wk", 3 * 365 * 86400L);
            case "5y" -> new TimeframeSpec("5y", "1wk", 0);
            case "max", "all" -> new TimeframeSpec("max", "1mo", 0);
            default -> new TimeframeSpec("1d", "1m", 0);
        };
    }

    public static boolean isMarketOpenForSymbol(String symbol, ZoneId zone, Instant now) {
        if (zone == null) {
            zone = ZoneId.of("Asia/Kolkata");
        }
        if (now == null) {
            now = Instant.now();
        }
        ZonedDateTime zdt = now.atZone(zone);
        DayOfWeek day = zdt.getDayOfWeek();
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) {
            return false;
        }
        LocalTime time = zdt.toLocalTime();
        LocalTime open = LocalTime.of(9, 15);
        LocalTime close = LocalTime.of(15, 30);
        return !time.isBefore(open) && !time.isAfter(close);
    }

    public boolean isMarketOpen(String symbol) {
        return isMarketOpenForSymbol(symbol, ZoneId.of("Asia/Kolkata"), Instant.now());
    }

    public CompletableFuture<String> fetchChartHistoryJson(String symbol, String timeframe) {
        if (symbol == null || symbol.isBlank()) {
            return CompletableFuture.completedFuture("{\"error\":\"Invalid symbol\"}");
        }
        String cleanSymbol = symbol.trim().toUpperCase();
        TimeframeSpec spec = resolveTimeframeSpec(timeframe);
        String encoded = URLEncoder.encode(cleanSymbol, StandardCharsets.UTF_8);
        String url = baseUrl + "/v8/finance/chart/" + encoded + "?range=" + spec.range() + "&interval=" + spec.interval();

        HttpClient client = (this.httpClient != null) ? this.httpClient : HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) track-my-market/1.0")
            .header("Accept", "application/json")
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply(resp -> {
                if (resp.statusCode() == 200) {
                    return parseChartJsonToCompactJson(cleanSymbol, timeframe, spec, resp.body());
                } else {
                    return "{\"symbol\":\"" + escapeJson(cleanSymbol) + "\",\"timeframe\":\"" + escapeJson(timeframe)
                        + "\",\"isTradable\":false,\"marketState\":\"DELISTED\",\"points\":[]}";
                }
            })
            .exceptionally(ex -> "{\"error\":\"" + escapeJson(ex.getMessage()) + "\"}");
    }

    public String parseChartJsonToCompactJson(String symbol, String timeframe, TimeframeSpec spec, String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return "{\"symbol\":\"" + escapeJson(symbol) + "\",\"timeframe\":\"" + escapeJson(timeframe) + "\",\"points\":[]}";
        }

        String currency = "INR";
        Matcher currM = CURRENCY_PATTERN.matcher(rawJson);
        if (currM.find()) {
            currency = currM.group(1);
        }

        Matcher longM = LONGNAME_PATTERN.matcher(rawJson);
        if (longM.find()) {
            registerCompanyName(symbol, longM.group(1));
        }

        Double previousClose = null;
        Matcher prevM = PREV_CLOSE_PATTERN.matcher(rawJson);
        if (prevM.find()) {
            try {
                previousClose = Double.parseDouble(prevM.group(1));
            } catch (NumberFormatException ignored) {}
        }

        List<Long> timestamps = Collections.emptyList();
        Matcher timeM = TIMESTAMPS_PATTERN.matcher(rawJson);
        if (timeM.find()) {
            timestamps = parseLongCsv(timeM.group(1));
        }

        List<Double> closes = Collections.emptyList();
        Matcher closeM = CLOSE_ARRAY_PATTERN.matcher(rawJson);
        if (closeM.find()) {
            closes = parseDoubleCsv(closeM.group(1));
        }

        List<Double> highs = Collections.emptyList();
        Matcher highM = HIGH_ARRAY_PATTERN.matcher(rawJson);
        if (highM.find()) {
            highs = parseDoubleCsv(highM.group(1));
        }

        List<Double> lows = Collections.emptyList();
        Matcher lowM = LOW_ARRAY_PATTERN.matcher(rawJson);
        if (lowM.find()) {
            lows = parseDoubleCsv(lowM.group(1));
        }

        long maxTs = 0;
        for (Long t : timestamps) {
            if (t != null && t > maxTs) {
                maxTs = t;
            }
        }
        long cutoff = (spec != null && spec.maxAgeSeconds() > 0 && maxTs > 0)
            ? (maxTs - spec.maxAgeSeconds()) : 0;

        List<ChartPoint> points = new ArrayList<>();
        Double minLow = null;
        Double maxHigh = null;

        int len = Math.min(timestamps.size(), closes.size());
        for (int i = 0; i < len; i++) {
            Long t = timestamps.get(i);
            Double c = closes.get(i);
            if (t == null || c == null) {
                continue;
            }
            if (cutoff > 0 && t < cutoff) {
                continue;
            }

            Double h = (i < highs.size()) ? highs.get(i) : null;
            Double l = (i < lows.size()) ? lows.get(i) : null;

            double pointHigh = (h != null) ? h : c;
            double pointLow = (l != null) ? l : c;

            if (maxHigh == null || pointHigh > maxHigh) {
                maxHigh = pointHigh;
            }
            if (minLow == null || pointLow < minLow) {
                minLow = pointLow;
            }

            points.add(new ChartPoint(t, c, null, h, l, c, null));
        }

        Double firstPrice = points.isEmpty() ? null : points.get(0).value();
        Double lastPrice = points.isEmpty() ? null : points.get(points.size() - 1).value();

        if (maxHigh == null && !points.isEmpty()) {
            maxHigh = points.stream().mapToDouble(ChartPoint::value).max().orElse(0.0);
        }
        if (minLow == null && !points.isEmpty()) {
            minLow = points.stream().mapToDouble(ChartPoint::value).min().orElse(0.0);
        }

        boolean isError = symbolErrors.containsKey(symbol);
        boolean isTradable = !isError && isMarketOpen(symbol);
        String marketState = isError ? "DELISTED" : (isTradable ? "OPEN" : "CLOSED");
        String companyName = getCompanyName(symbol);

        StringBuilder sb = new StringBuilder("{");
        sb.append("\"symbol\":\"").append(escapeJson(symbol)).append("\",");
        sb.append("\"companyName\":\"").append(escapeJson(companyName)).append("\",");
        sb.append("\"timeframe\":\"").append(escapeJson(timeframe != null ? timeframe : "1d")).append("\",");
        sb.append("\"range\":\"").append(escapeJson(spec != null ? spec.range() : "1d")).append("\",");
        sb.append("\"interval\":\"").append(escapeJson(spec != null ? spec.interval() : "1m")).append("\",");
        sb.append("\"currency\":\"").append(escapeJson(currency)).append("\",");
        sb.append("\"isTradable\":").append(isTradable).append(",");
        sb.append("\"marketState\":\"").append(marketState).append("\",");
        if (previousClose != null) {
            sb.append("\"previousClose\":").append(previousClose).append(",");
        }
        if (firstPrice != null) {
            sb.append("\"firstPrice\":").append(firstPrice).append(",");
        }
        if (lastPrice != null) {
            sb.append("\"lastPrice\":").append(lastPrice).append(",");
        }
        if (maxHigh != null) {
            sb.append("\"highPrice\":").append(maxHigh).append(",");
        }
        if (minLow != null) {
            sb.append("\"lowPrice\":").append(minLow).append(",");
        }

        sb.append("\"points\":[");
        for (int i = 0; i < points.size(); i++) {
            if (i > 0) sb.append(",");
            ChartPoint p = points.get(i);
            sb.append("{\"time\":").append(p.time()).append(",\"value\":").append(p.value()).append("}");
        }
        sb.append("]}");
        return sb.toString();
    }

    private static List<Long> parseLongCsv(String csv) {
        if (csv == null || csv.isBlank()) return Collections.emptyList();
        List<Long> list = new ArrayList<>();
        for (String part : csv.split(",")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty() || trimmed.equalsIgnoreCase("null")) {
                list.add(null);
            } else {
                try {
                    list.add(Long.parseLong(trimmed));
                } catch (NumberFormatException e) {
                    list.add(null);
                }
            }
        }
        return list;
    }

    private static List<Double> parseDoubleCsv(String csv) {
        if (csv == null || csv.isBlank()) return Collections.emptyList();
        List<Double> list = new ArrayList<>();
        for (String part : csv.split(",")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty() || trimmed.equalsIgnoreCase("null")) {
                list.add(null);
            } else {
                try {
                    list.add(Double.parseDouble(trimmed));
                } catch (NumberFormatException e) {
                    list.add(null);
                }
            }
        }
        return list;
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
