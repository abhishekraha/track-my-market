package market.consumer;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import market.sbe.MarketTickDecoder;
import market.sbe.MessageHeaderDecoder;
import org.agrona.DirectBuffer;

import java.io.PrintStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Consumer that polls MarketTick messages from the Aeron bus, decodes them via SBE,
 * measures microsecond/nanosecond transport latency, and renders them to output.
 */
public class MarketDisplayConsumer implements FragmentHandler {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(ZoneOffset.UTC);

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final MarketTickDecoder tickDecoder = new MarketTickDecoder();
    private final PrintStream out;
    private final ConcurrentHashMap<String, MarketTickView> latestTicks = new ConcurrentHashMap<>();
    private final AtomicLong tickCount = new AtomicLong();
    private Consumer<MarketTickView> tickListener;

    public record MarketTickView(
        String symbol,
        String isin,
        String sedol,
        String ric,
        double price,
        long quoteTimestampNs,
        long ingressTimestampNs,
        long fetchDurationNs,
        long busLatencyNs,
        String provider
    ) {}

    public MarketDisplayConsumer() {
        this(System.out);
    }

    public MarketDisplayConsumer(PrintStream out) {
        this.out = out;
    }

    public void setTickListener(Consumer<MarketTickView> tickListener) {
        this.tickListener = tickListener;
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.templateId() != MarketTickDecoder.TEMPLATE_ID) {
            return;
        }

        tickDecoder.wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            headerDecoder.blockLength(),
            headerDecoder.version()
        );

        Instant now = Instant.now();
        long nowEpochNs = now.getEpochSecond() * 1_000_000_000L + now.getNano();
        long ingressNs = tickDecoder.ingressTimestampNs();
        long busLatencyNs = Math.max(0, nowEpochNs - ingressNs);

        double price = tickDecoder.price() / 10_000.0;
        String symbol = tickDecoder.symbol().value().trim();
        String isin = tickDecoder.isin().value().trim();
        String sedol = tickDecoder.sedol().value().trim();
        String ric = tickDecoder.ric().value().trim();
        double fetchDurationMs = tickDecoder.fetchDurationNs() / 1_000_000.0;
        double busLatencyUs = busLatencyNs / 1_000.0;

        MarketTickView view = new MarketTickView(
            symbol,
            isin,
            sedol,
            ric,
            price,
            tickDecoder.quoteTimestampNs(),
            ingressNs,
            tickDecoder.fetchDurationNs(),
            busLatencyNs,
            tickDecoder.provider().name()
        );

        latestTicks.put(symbol, view);
        tickCount.incrementAndGet();

        if (out != null) {
            String timeStr = TIME_FORMATTER.format(Instant.ofEpochMilli(tickDecoder.quoteTimestampNs() / 1_000_000L));
            out.printf(
                "[%s UTC] TICK: %-12s | Price: ₹%,10.2f | ISIN: %-12s | Fetch: %6.1f ms | Bus Latency: %6.2f µs | Provider: %s%n",
                timeStr, symbol, price, isin, fetchDurationMs, busLatencyUs, tickDecoder.provider()
            );
        }

        if (tickListener != null) {
            tickListener.accept(view);
        }
    }

    public MarketTickView getLatestTick(String symbol) {
        return latestTicks.get(symbol);
    }

    public long getTickCount() {
        return tickCount.get();
    }
}
