package market.web;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import market.sbe.MarketTickDecoder;
import market.sbe.MessageHeaderDecoder;
import market.sbe.ModelProvider;
import org.agrona.DirectBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Aeron FragmentHandler that decodes SBE market ticks from the IPC bus,
 * computes bus transport latency, caches the latest quote per symbol,
 * and broadcasts SSE (Server-Sent Events) to active HTTP client connections.
 */
public class MarketStreamBroadcaster implements FragmentHandler {

    public record MarketTickView(
        String symbol,
        String isin,
        String sedol,
        String ric,
        double price,
        long quoteTimestampNs,
        long ingressTimestampNs,
        long fetchDurationNs,
        double busLatencyUs,
        String provider
    ) {}

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final MarketTickDecoder tickDecoder = new MarketTickDecoder();

    private final CopyOnWriteArrayList<OutputStream> clients = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, MarketTickView> latestQuotes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> latestJsonQuotes = new ConcurrentHashMap<>();

    public void registerClient(OutputStream clientOut) {
        if (clientOut != null) {
            clients.addIfAbsent(clientOut);
        }
    }

    public void unregisterClient(OutputStream clientOut) {
        if (clientOut != null) {
            clients.remove(clientOut);
        }
    }

    public int getActiveClientCount() {
        return clients.size();
    }

    public MarketTickView getLatestQuote(String symbol) {
        return latestQuotes.get(symbol);
    }

    public Map<String, MarketTickView> getLatestQuotes() {
        return Collections.unmodifiableMap(latestQuotes);
    }

    public String getInitialSnapshotJson() {
        if (latestJsonQuotes.isEmpty()) {
            return "[]";
        }
        return "[" + String.join(",", latestJsonQuotes.values()) + "]";
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        if (buffer == null || length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }

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
        long ingressTimestampNs = tickDecoder.ingressTimestampNs();
        double busLatencyUs = Math.max(0.0, (nowEpochNs - ingressTimestampNs) / 1000.0);

        double price = tickDecoder.price() / 10_000.0;
        long quoteTimestampNs = tickDecoder.quoteTimestampNs();
        long quoteTime = quoteTimestampNs / 1_000_000L;
        long fetchDurationNs = tickDecoder.fetchDurationNs();
        double fetchDurationMs = fetchDurationNs / 1_000_000.0;

        String symbol = tickDecoder.symbol().value().trim();
        String isin = tickDecoder.isin().value().trim();
        String sedol = tickDecoder.sedol().value().trim();
        String ric = tickDecoder.ric().value().trim();
        ModelProvider providerEnum = tickDecoder.provider();
        String provider = (providerEnum != null && providerEnum != ModelProvider.NULL_VAL)
            ? providerEnum.name()
            : (providerEnum != null ? providerEnum.name() : "");

        MarketTickView view = new MarketTickView(
            symbol,
            isin,
            sedol,
            ric,
            price,
            quoteTimestampNs,
            ingressTimestampNs,
            fetchDurationNs,
            busLatencyUs,
            provider
        );

        String json = formatQuoteJson(
            symbol,
            price,
            isin,
            sedol,
            ric,
            quoteTime,
            fetchDurationMs,
            busLatencyUs,
            provider
        );

        latestQuotes.put(symbol, view);
        latestJsonQuotes.put(symbol, json);

        broadcastTick(json);
    }

    public void broadcastSymbolStatus(String symbol, String status, String message) {
        String json = String.format("{\"symbol\":\"%s\",\"status\":\"%s\",\"message\":\"%s\"}",
            escapeJson(symbol), escapeJson(status), escapeJson(message));
        broadcastEvent("symbol-status", json);
    }

    public void broadcastEvent(String eventName, String dataJson) {
        if (clients.isEmpty()) {
            return;
        }

        byte[] payload = ("event: " + eventName + "\ndata: " + dataJson + "\n\n").getBytes(StandardCharsets.UTF_8);

        for (OutputStream client : clients) {
            try {
                synchronized (client) {
                    client.write(payload);
                    client.flush();
                }
            } catch (IOException | RuntimeException e) {
                clients.remove(client);
                try {
                    client.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private void broadcastTick(String json) {
        broadcastEvent("tick", json);
    }

    private static String formatQuoteJson(
        String symbol,
        double price,
        String isin,
        String sedol,
        String ric,
        long quoteTime,
        double fetchDurationMs,
        double busLatencyUs,
        String provider
    ) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("{\"symbol\":\"").append(escapeJson(symbol)).append("\",")
          .append("\"price\":").append(price).append(",")
          .append("\"isin\":\"").append(escapeJson(isin)).append("\",")
          .append("\"sedol\":\"").append(escapeJson(sedol)).append("\",")
          .append("\"ric\":\"").append(escapeJson(ric)).append("\",")
          .append("\"quoteTime\":").append(quoteTime).append(",")
          .append("\"fetchDurationMs\":").append(fetchDurationMs).append(",")
          .append("\"busLatencyUs\":").append(busLatencyUs).append(",")
          .append("\"provider\":\"").append(escapeJson(provider)).append("\"}");
        return sb.toString();
    }

    private static String escapeJson(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
