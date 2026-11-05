package market.provider;

import com.sun.net.httpserver.HttpServer;
import market.provider.yahoo.YahooFinanceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class YahooFinanceProviderTest {

    @AfterEach
    void resetSystemProperties() {
        System.clearProperty(YahooFinanceProvider.SYSTEM_PROPERTY_BASE_URL);
        System.clearProperty("market.provider.yahoo.poll-interval-ms");
    }

    @Test
    void shouldParseYahooJsonQuoteAndNotifyListener() {
        String mockJsonResponse = """
        {
          "chart": {
            "result": [{
              "meta": {
                "symbol": "RELIANCE.NS",
                "regularMarketPrice": 2500.50,
                "regularMarketTime": 1730790000
              }
            }]
          }
        }
        """;

        List<RawMarketQuote> quotes = new ArrayList<>();
        YahooFinanceProvider provider = new YahooFinanceProvider();
        provider.registerTickListener(quotes::add);

        provider.parseAndEmit("RELIANCE.NS", mockJsonResponse, 5_000_000L);

        assertThat(quotes).hasSize(1);
        RawMarketQuote quote = quotes.get(0);
        assertThat(quote.symbol()).isEqualTo("RELIANCE.NS");
        assertThat(quote.price()).isEqualTo(2500.50);
        assertThat(quote.quoteTimestampEpochNs()).isEqualTo(1730790000L * 1_000_000_000L);
        assertThat(quote.fetchDurationNs()).isEqualTo(5_000_000L);
        assertThat(quote.isin()).isEqualTo("INE002A01018");
        assertThat(quote.sedol()).isEqualTo("6714072");
        assertThat(quote.ric()).isEqualTo("RELI.NS");
    }

    @Test
    void shouldSuppressDuplicateQuotes() {
        String mockResponse = """
        {
          "chart": {
            "result": [{
              "meta": {
                "symbol": "TCS.NS",
                "regularMarketPrice": 3900.00,
                "regularMarketTime": 1730790000
              }
            }]
          }
        }
        """;

        List<RawMarketQuote> quotes = new ArrayList<>();
        YahooFinanceProvider provider = new YahooFinanceProvider();
        provider.registerTickListener(quotes::add);

        provider.parseAndEmit("TCS.NS", mockResponse, 2_000_000L);
        provider.parseAndEmit("TCS.NS", mockResponse, 2_000_000L); // Duplicate

        assertThat(quotes).hasSize(1); // Deduped!
    }

    @Test
    void shouldEmitWhenPriceOrTimestampChanges() {
        String tick1 = """
        {
          "chart": {
            "result": [{
              "meta": {
                "symbol": "INFY.NS",
                "regularMarketPrice": 1850.25,
                "regularMarketTime": 1730790000
              }
            }]
          }
        }
        """;

        String tick2 = """
        {
          "chart": {
            "result": [{
              "meta": {
                "symbol": "INFY.NS",
                "regularMarketPrice": 1852.50,
                "regularMarketTime": 1730790005
              }
            }]
          }
        }
        """;

        List<RawMarketQuote> quotes = new ArrayList<>();
        YahooFinanceProvider provider = new YahooFinanceProvider();
        provider.registerTickListener(quotes::add);

        provider.parseAndEmit("INFY.NS", tick1, 1_000_000L);
        provider.parseAndEmit("INFY.NS", tick2, 1_200_000L);

        assertThat(quotes).hasSize(2);
        assertThat(quotes.get(0).price()).isEqualTo(1850.25);
        assertThat(quotes.get(1).price()).isEqualTo(1852.50);
        assertThat(quotes.get(1).quoteTimestampEpochNs()).isEqualTo(1730790005L * 1_000_000_000L);
    }

    @Test
    void shouldConvertSecondsToEpochNanoseconds() {
        String json = """
        {
          "chart": {
            "result": [{
              "meta": {
                "regularMarketPrice": 100.0,
                "regularMarketTime": 1700000000
              }
            }]
          }
        }
        """;

        List<RawMarketQuote> quotes = new ArrayList<>();
        YahooFinanceProvider provider = new YahooFinanceProvider();
        provider.registerTickListener(quotes::add);

        provider.parseAndEmit("TEST.NS", json, 100_000L);

        assertThat(quotes).hasSize(1);
        assertThat(quotes.get(0).quoteTimestampEpochNs()).isEqualTo(1700000000_000_000_000L);
    }

    @Test
    void shouldConfigureBaseUrlFromConstructor() {
        YahooFinanceProvider provider = new YahooFinanceProvider("http://nginx-proxy:8080/proxy/");
        assertThat(provider.getBaseUrl()).isEqualTo("http://nginx-proxy:8080/proxy");

        String uri = provider.buildSymbolUri("RELIANCE.NS");
        assertThat(uri).isEqualTo("http://nginx-proxy:8080/proxy/v8/finance/chart/RELIANCE.NS");
    }

    @Test
    void shouldHandleBaseUrlWithChartPathAlreadyPresent() {
        YahooFinanceProvider provider = new YahooFinanceProvider("http://nginx-proxy:8080/v8/finance/chart");
        String uri = provider.buildSymbolUri("RELIANCE.NS");
        assertThat(uri).isEqualTo("http://nginx-proxy:8080/v8/finance/chart/RELIANCE.NS");
    }

    @Test
    void shouldConfigureBaseUrlFromSystemProperty() {
        System.setProperty(YahooFinanceProvider.SYSTEM_PROPERTY_BASE_URL, "https://my-internal-yahoo-cache.corp");
        YahooFinanceProvider provider = new YahooFinanceProvider();
        assertThat(provider.getBaseUrl()).isEqualTo("https://my-internal-yahoo-cache.corp");
    }

    @Test
    void shouldFallbackToDefaultBaseUrl() {
        YahooFinanceProvider provider = new YahooFinanceProvider();
        assertThat(provider.getBaseUrl()).isEqualTo(YahooFinanceProvider.DEFAULT_BASE_URL);
    }

    @Test
    void shouldProvideSensibleDefaultsForUnmappedSymbols() {
        YahooFinanceProvider provider = new YahooFinanceProvider();
        var ids = provider.getIdentifiers("CUSTOM.NS");

        assertThat(ids.isin()).isEqualTo("UNKNOWN");
        assertThat(ids.sedol()).isEqualTo("UNKNOWN");
        assertThat(ids.ric()).isEqualTo("CUSTOM.NS");
    }

    @Test
    void shouldAllowCustomIdentifierRegistration() {
        YahooFinanceProvider provider = new YahooFinanceProvider();
        provider.registerIdentifierMapping("HDFCBANK.NS", "INE040A01034", "6415526", "HDBK.NS");

        var ids = provider.getIdentifiers("HDFCBANK.NS");
        assertThat(ids.isin()).isEqualTo("INE040A01034");
        assertThat(ids.sedol()).isEqualTo("6415526");
        assertThat(ids.ric()).isEqualTo("HDBK.NS");
    }

    @Test
    void shouldGracefullyHandleMalformedOrEmptyJson() {
        List<RawMarketQuote> quotes = new ArrayList<>();
        YahooFinanceProvider provider = new YahooFinanceProvider();
        provider.registerTickListener(quotes::add);

        provider.parseAndEmit("RELIANCE.NS", null, 1000L);
        provider.parseAndEmit("RELIANCE.NS", "", 1000L);
        provider.parseAndEmit("RELIANCE.NS", "{\"chart\":{\"result\":null,\"error\":{\"description\":\"Not found\"}}}", 1000L);
        provider.parseAndEmit("RELIANCE.NS", "{ broken json", 1000L);

        assertThat(quotes).isEmpty();
    }

    @Test
    void shouldPollAndEmitQuotesOverHttpUsingLocalServer() throws Exception {
        String mockResponse = """
        {
          "chart": {
            "result": [{
              "meta": {
                "symbol": "RELIANCE.NS",
                "regularMarketPrice": 2550.75,
                "regularMarketTime": 1730791234
              }
            }]
          }
        }
        """;

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v8/finance/chart/RELIANCE.NS", exchange -> {
            byte[] bytes = mockResponse.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        server.start();

        int port = server.getAddress().getPort();
        String testBaseUrl = "http://127.0.0.1:" + port;

        CountDownLatch latch = new CountDownLatch(1);
        List<RawMarketQuote> receivedQuotes = Collections.synchronizedList(new ArrayList<>());

        try (YahooFinanceProvider provider = new YahooFinanceProvider(
            testBaseUrl,
            null,
            null,
            Duration.ofMillis(200)
        )) {
            provider.registerTickListener(quote -> {
                receivedQuotes.add(quote);
                latch.countDown();
            });

            provider.subscribe(List.of("RELIANCE.NS"));
            assertThat(provider.getSubscribedSymbols()).containsExactly("RELIANCE.NS");

            provider.start();
            assertThat(provider.isRunning()).isTrue();

            boolean received = latch.await(5, TimeUnit.SECONDS);
            assertThat(received).isTrue();

            assertThat(receivedQuotes).hasSize(1);
            RawMarketQuote quote = receivedQuotes.get(0);
            assertThat(quote.symbol()).isEqualTo("RELIANCE.NS");
            assertThat(quote.price()).isEqualTo(2550.75);
            assertThat(quote.quoteTimestampEpochNs()).isEqualTo(1730791234L * 1_000_000_000L);
            assertThat(quote.isin()).isEqualTo("INE002A01018");

            provider.stop();
            assertThat(provider.isRunning()).isFalse();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void shouldParseSearchResultsJson() {
        String mockSearchJson = """
        {
          "count": 2,
          "quotes": [
            {
              "exchange": "NSI",
              "shortname": "TATA CONSULTANCY SERV LT",
              "quoteType": "EQUITY",
              "symbol": "TCS.NS",
              "longname": "Tata Consultancy Services Limited",
              "exchDisp": "NSE"
            },
            {
              "exchange": "NSI",
              "shortname": "TATA STEEL LIMITED",
              "quoteType": "EQUITY",
              "symbol": "TATASTEEL.NS",
              "longname": "Tata Steel Limited",
              "exchDisp": "NSE"
            }
          ]
        }
        """;

        YahooFinanceProvider provider = new YahooFinanceProvider();
        List<YahooFinanceProvider.SearchResult> results = provider.parseSearchResults(mockSearchJson);

        assertThat(results).hasSize(2);
        assertThat(results.get(0).symbol()).isEqualTo("TCS.NS");
        assertThat(results.get(0).name()).isEqualTo("Tata Consultancy Services Limited");
        assertThat(results.get(0).exchange()).isEqualTo("NSE");
        assertThat(results.get(0).type()).isEqualTo("EQUITY");

        assertThat(results.get(1).symbol()).isEqualTo("TATASTEEL.NS");
        assertThat(results.get(1).name()).isEqualTo("Tata Steel Limited");
    }

    @Test
    void shouldRegisterTmpvAndTmcvIdentifierMappings() {
        YahooFinanceProvider provider = new YahooFinanceProvider();
        var tmpv = provider.getIdentifiers("TMPV.NS");
        assertThat(tmpv.isin()).isEqualTo("INE155A01022");
        assertThat(tmpv.ric()).isEqualTo("TMPV.NS");

        var tmcv = provider.getIdentifiers("TMCV.NS");
        assertThat(tmcv.isin()).isEqualTo("INE155A01022");
        assertThat(tmcv.ric()).isEqualTo("TMCV.NS");
    }

    @Test
    void shouldNotifyErrorListenerWhenSymbolDelisted() {
        YahooFinanceProvider provider = new YahooFinanceProvider();
        List<String> errors = new ArrayList<>();
        provider.registerErrorListener((sym, err) -> errors.add(sym + ": " + err));

        String delistedJson = "{\"chart\":{\"result\":null,\"error\":{\"code\":\"Not Found\",\"description\":\"No data found, symbol may be delisted\"}}}";
        provider.handleSymbolErrorResponse("TATAMOTORS.NS", delistedJson);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("TATAMOTORS.NS");
        assertThat(errors.get(0)).contains("TMPV.NS");
        assertThat(provider.getSymbolErrors()).containsKey("TATAMOTORS.NS");
    }
}
