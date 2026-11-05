package market;

import market.bus.aeron.AeronEventBus;
import market.consumer.MarketDisplayConsumer;
import market.producer.MarketDataProducer;
import market.provider.yahoo.YahooFinanceProvider;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main application entrypoint for Phase 1 of track-my-market:
 * Ingests NSE/BSE stock quotes, serializes to zero-allocation SBE messages,
 * streams them over an embedded Aeron Archiving Media Driver message bus,
 * and displays live tick updates with microsecond latency metrics.
 */
public class MarketApp {
    public static final int DEFAULT_STREAM_ID = 1001;

    public static void main(String[] args) {
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

        System.out.println("======================================================================");
        System.out.println("               TRACK-MY-MARKET: Phase 1 Ingestion & Bus               ");
        System.out.println("======================================================================");
        System.out.printf("  • Provider: Yahoo Finance (Endpoint: %s)%n", customBaseUrl);
        System.out.printf("  • Symbols:  %s%n", symbols);
        System.out.printf("  • Interval: %d ms%n", pollIntervalMs);
        System.out.println("  • Bus:      Aeron Archiving Media Driver (IPC / SBE)");
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

        AtomicBoolean running = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down track-my-market...");
            running.set(false);
            provider.stop();
            eventBus.close();
            System.out.println("Shutdown complete.");
        }));

        try {
            eventBus.start();
            eventBus.subscribe(DEFAULT_STREAM_ID, displayConsumer);

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
