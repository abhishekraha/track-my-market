package market;

import market.alert.AlertController;
import market.alert.AlertJsonCodec;
import market.alert.AlertRepository;
import market.alert.AlertRuleEngine;
import market.alert.TelegramDispatcher;
import market.bus.aeron.AeronEventBus;
import market.consumer.MarketDisplayConsumer;
import market.producer.MarketDataProducer;
import market.provider.yahoo.YahooFinanceProvider;
import market.web.DynamicWatchlistController;
import market.web.MarketHttpServer;
import market.web.MarketStreamBroadcaster;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main application entrypoint for track-my-market:
 * Ingests NSE/BSE stock quotes, serializes to zero-allocation SBE messages,
 * streams them over an embedded Aeron Archiving Media Driver message bus,
 * displays live tick updates in console, and optionally serves a modern
 * real-time web trading dashboard via SSE when --enable-ui is passed.
 */
public class MarketApp {
    public static final int DEFAULT_STREAM_ID = 1001;

    public static void main(String[] args) {
        List<String> argList = Arrays.asList(args);
        boolean enableUi = argList.contains("--enable-ui")
            || Boolean.parseBoolean(System.getProperty("market.ui.enabled",
                System.getenv().getOrDefault("MARKET_UI_ENABLED", "false")));

        String symbolsProperty = System.getProperty("market.symbols",
            System.getenv().getOrDefault("MARKET_SYMBOLS", "RELIANCE.NS,TCS.NS,INFY.NS,HDFCBANK.NS"));
        List<String> symbols = Arrays.stream(symbolsProperty.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toList();

        long pollIntervalMs = Long.parseLong(System.getProperty("market.poll.interval.ms",
            System.getenv().getOrDefault("MARKET_POLL_INTERVAL_MS", "3000")));

        String customBaseUrl = System.getProperty("market.provider.yahoo.base-url",
            System.getenv().getOrDefault("YAHOO_FINANCE_BASE_URL", "https://query1.finance.yahoo.com"));

        int serverPort = Integer.parseInt(System.getProperty("market.server.port",
            System.getenv().getOrDefault("MARKET_SERVER_PORT", "8080")));
        String bindHost = System.getProperty("market.server.bind-host",
            System.getenv().getOrDefault("MARKET_SERVER_BIND_HOST", "0.0.0.0"));
        String contextPath = System.getProperty("market.server.context-path",
            System.getenv().getOrDefault("MARKET_SERVER_CONTEXT_PATH", "/"));

        System.out.println("======================================================================");
        System.out.println("                         TRACK-MY-MARKET                              ");
        System.out.println("======================================================================");
        System.out.printf("  • Provider: Yahoo Finance (Endpoint: %s)%n", customBaseUrl);
        System.out.printf("  • Symbols:  %s%n", symbols);
        System.out.printf("  • Interval: %d ms%n", pollIntervalMs);
        System.out.println("  • Bus:      Aeron Archiving Media Driver (IPC / SBE)");
        System.out.printf("  • Web UI:   %s%n", enableUi ? "ENABLED" : "DISABLED (Headless)");
        if (enableUi) {
            String displayPath = contextPath.endsWith("/") ? contextPath : contextPath + "/";
            System.out.printf("  • Dashboard URL: http://%s:%d%s%n",
                "0.0.0.0".equals(bindHost) ? "localhost" : bindHost, serverPort, displayPath);
            System.out.printf("  • SSE Stream:    http://%s:%d%sapi/stream%n",
                "0.0.0.0".equals(bindHost) ? "localhost" : bindHost, serverPort, displayPath);
        }
        System.out.println("======================================================================\n");

        Path mediaDriverDir;
        Path archiveDir;
        try {
            mediaDriverDir = Files.createTempDirectory("aeron-driver-");
            archiveDir = Files.createTempDirectory("aeron-archive-");
        } catch (Exception e) {
            throw new RuntimeException("Failed to create temporary Aeron directory", e);
        }

        AeronEventBus eventBus = new AeronEventBus(
            "aeron:ipc",
            DEFAULT_STREAM_ID,
            mediaDriverDir.toFile(),
            archiveDir.toFile(),
            true,
            new org.agrona.concurrent.YieldingIdleStrategy(),
            100
        );

        MarketDisplayConsumer displayConsumer = new MarketDisplayConsumer();
        MarketDataProducer producer = new MarketDataProducer(eventBus, DEFAULT_STREAM_ID);
        YahooFinanceProvider provider = new YahooFinanceProvider(
            customBaseUrl,
            null,
            null,
            Duration.ofMillis(pollIntervalMs)
        );

        AlertRepository alertRepository = new AlertRepository(AlertRepository.DEFAULT_STORAGE_PATH, provider);
        TelegramDispatcher telegramDispatcher = new TelegramDispatcher();
        AlertRuleEngine alertEngine = new AlertRuleEngine(alertRepository, telegramDispatcher);

        MarketHttpServer webServer = null;
        MarketStreamBroadcaster broadcaster = null;

        if (enableUi) {
            broadcaster = new MarketStreamBroadcaster();
            DynamicWatchlistController watchlistController = new DynamicWatchlistController(provider);
            AlertController alertController = new AlertController(
                alertRepository,
                alertEngine,
                telegramDispatcher,
                provider
            );
            try {
                webServer = new MarketHttpServer(
                    bindHost,
                    serverPort,
                    contextPath,
                    broadcaster,
                    watchlistController,
                    eventBus.telemetryCollector(),
                    alertController
                );
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize embedded web server", e);
            }
        }

        AtomicBoolean running = new AtomicBoolean(true);
        final MarketHttpServer finalServer = webServer;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down track-my-market...");
            running.set(false);
            if (finalServer != null) {
                finalServer.stop();
            }
            alertRepository.close();
            provider.stop();
            eventBus.close();
            System.out.println("Shutdown complete.");
        }));

        try {
            eventBus.start();
            eventBus.subscribe(DEFAULT_STREAM_ID, displayConsumer);
            eventBus.subscribe(DEFAULT_STREAM_ID, alertEngine);

            if (enableUi && broadcaster != null) {
                MarketStreamBroadcaster sseBroadcaster = broadcaster;
                provider.registerErrorListener((sym, reason) ->
                    sseBroadcaster.broadcastSymbolStatus(sym, "ERROR", reason));
                alertEngine.registerListener((rule, event) ->
                    sseBroadcaster.broadcastEvent("alert", AlertJsonCodec.serializeEvent(rule, event)));
                eventBus.subscribe(DEFAULT_STREAM_ID, broadcaster);
                webServer.start();
            }

            provider.registerTickListener(producer::onQuote);
            provider.subscribe(symbols);
            provider.start();

            SleepingMillisIdleStrategy idleStrategy = new SleepingMillisIdleStrategy(1);
            while (running.get()) {
                int fragments = eventBus.poll(10);
                idleStrategy.idle(fragments);
            }
        } catch (Exception e) {
            if (running.get()) {
                System.err.println("Fatal error in MarketApp: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
