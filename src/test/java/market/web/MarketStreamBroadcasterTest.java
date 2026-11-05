package market.web;

import market.sbe.MarketTickEncoder;
import market.sbe.MessageHeaderEncoder;
import market.sbe.ModelProvider;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

class MarketStreamBroadcasterTest {

    @Test
    @DisplayName("Should decode SBE tick and broadcast SSE event to registered clients")
    void shouldDecodeSbeTickAndBroadcastSseEventToRegisteredClients() {
        MarketStreamBroadcaster broadcaster = new MarketStreamBroadcaster();
        ByteArrayOutputStream clientOut = new ByteArrayOutputStream();

        broadcaster.registerClient(clientOut);
        assertThat(broadcaster.getActiveClientCount()).isEqualTo(1);

        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        new MarketTickEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .price(25005000L) // 2500.50
            .quoteTimestampNs(1730790000000000000L)
            .ingressTimestampNs(System.currentTimeMillis() * 1_000_000L)
            .fetchDurationNs(10_000_000L)
            .provider(ModelProvider.YAHOO)
            .symbol().value("RELIANCE.NS");

        broadcaster.onFragment(buffer, 0, 256, null);

        String output = clientOut.toString();
        assertThat(output).contains("event: tick\n");
        assertThat(output).contains("\"symbol\":\"RELIANCE.NS\"");
        assertThat(output).contains("\"price\":2500.5");

        String snapshot = broadcaster.getInitialSnapshotJson();
        assertThat(snapshot).contains("RELIANCE.NS");
        assertThat(snapshot).contains("2500.5");
    }

    @Test
    @DisplayName("Should remove dead client when IOException occurs without breaking broadcast to active clients")
    void shouldHandleDeadClientGracefully() {
        MarketStreamBroadcaster broadcaster = new MarketStreamBroadcaster();

        OutputStream failingClient = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("Connection reset by peer");
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                throw new IOException("Connection reset by peer");
            }
        };

        ByteArrayOutputStream healthyClient = new ByteArrayOutputStream();

        broadcaster.registerClient(failingClient);
        broadcaster.registerClient(healthyClient);
        assertThat(broadcaster.getActiveClientCount()).isEqualTo(2);

        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        new MarketTickEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .price(34502500L) // 3450.25
            .quoteTimestampNs(1730790000000000000L)
            .ingressTimestampNs(System.currentTimeMillis() * 1_000_000L)
            .fetchDurationNs(8_000_000L)
            .provider(ModelProvider.YAHOO)
            .symbol().value("TCS.NS");

        broadcaster.onFragment(buffer, 0, 256, null);

        // Failing client should have been automatically pruned
        assertThat(broadcaster.getActiveClientCount()).isEqualTo(1);
        assertThat(healthyClient.toString()).contains("\"symbol\":\"TCS.NS\"");
        assertThat(healthyClient.toString()).contains("\"price\":3450.25");
    }

    @Test
    @DisplayName("Should handle unregistering clients cleanly")
    void shouldUnregisterClient() {
        MarketStreamBroadcaster broadcaster = new MarketStreamBroadcaster();
        ByteArrayOutputStream client = new ByteArrayOutputStream();

        broadcaster.registerClient(client);
        assertThat(broadcaster.getActiveClientCount()).isEqualTo(1);

        broadcaster.unregisterClient(client);
        assertThat(broadcaster.getActiveClientCount()).isEqualTo(0);

        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        new MarketTickEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .price(15000000L)
            .quoteTimestampNs(1730790000000000000L)
            .ingressTimestampNs(System.currentTimeMillis() * 1_000_000L)
            .fetchDurationNs(5_000_000L)
            .provider(ModelProvider.YAHOO)
            .symbol().value("INFY.NS");

        broadcaster.onFragment(buffer, 0, 256, null);
        assertThat(client.size()).isEqualTo(0);
    }

    @Test
    @DisplayName("Should return empty JSON array when snapshot is empty")
    void shouldReturnEmptyJsonArrayWhenNoQuotesCached() {
        MarketStreamBroadcaster broadcaster = new MarketStreamBroadcaster();
        assertThat(broadcaster.getInitialSnapshotJson()).isEqualTo("[]");
    }

    @Test
    @DisplayName("Should broadcast symbol status event to active SSE clients")
    void shouldBroadcastSymbolStatusEvent() {
        MarketStreamBroadcaster broadcaster = new MarketStreamBroadcaster();
        ByteArrayOutputStream clientOut = new ByteArrayOutputStream();
        broadcaster.registerClient(clientOut);

        broadcaster.broadcastSymbolStatus("TATAMOTORS.NS", "ERROR", "Delisted: try TMPV.NS");

        String output = clientOut.toString();
        assertThat(output).contains("event: symbol-status\n");
        assertThat(output).contains("\"symbol\":\"TATAMOTORS.NS\"");
        assertThat(output).contains("\"status\":\"ERROR\"");
        assertThat(output).contains("TMPV.NS");
    }

    @Test
    @DisplayName("Should ignore fragments with unknown templateId or malformed buffer")
    void shouldIgnoreUnknownTemplateIdOrMalformedBuffer() {
        MarketStreamBroadcaster broadcaster = new MarketStreamBroadcaster();
        ByteArrayOutputStream client = new ByteArrayOutputStream();
        broadcaster.registerClient(client);

        // Null buffer or short buffer
        broadcaster.onFragment(null, 0, 0, null);
        UnsafeBuffer shortBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(4));
        broadcaster.onFragment(shortBuffer, 0, 4, null);

        // Unknown template ID (e.g. 9999)
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(64));
        MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        headerEncoder.wrap(buffer, 0)
            .blockLength(32)
            .templateId(9999)
            .schemaId(1)
            .version(1);

        broadcaster.onFragment(buffer, 0, 64, null);

        assertThat(client.size()).isEqualTo(0);
        assertThat(broadcaster.getInitialSnapshotJson()).isEqualTo("[]");
    }

    @Test
    @DisplayName("Should cache multiple quotes and return complete JSON array snapshot")
    void shouldCacheMultipleQuotes() {
        MarketStreamBroadcaster broadcaster = new MarketStreamBroadcaster();

        UnsafeBuffer buf1 = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        new MarketTickEncoder().wrapAndApplyHeader(buf1, 0, new MessageHeaderEncoder())
            .price(25000000L)
            .quoteTimestampNs(1730790000000000000L)
            .ingressTimestampNs(System.currentTimeMillis() * 1_000_000L)
            .fetchDurationNs(10_000_000L)
            .provider(ModelProvider.YAHOO)
            .symbol().value("RELIANCE.NS");
        broadcaster.onFragment(buf1, 0, 256, null);

        UnsafeBuffer buf2 = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        new MarketTickEncoder().wrapAndApplyHeader(buf2, 0, new MessageHeaderEncoder())
            .price(35000000L)
            .quoteTimestampNs(1730790000000000000L)
            .ingressTimestampNs(System.currentTimeMillis() * 1_000_000L)
            .fetchDurationNs(12_000_000L)
            .provider(ModelProvider.YAHOO)
            .symbol().value("TCS.NS");
        broadcaster.onFragment(buf2, 0, 256, null);

        String snapshot = broadcaster.getInitialSnapshotJson();
        assertThat(snapshot).startsWith("[").endsWith("]");
        assertThat(snapshot).contains("RELIANCE.NS");
        assertThat(snapshot).contains("TCS.NS");
        assertThat(broadcaster.getLatestQuote("RELIANCE.NS")).isNotNull();
        assertThat(broadcaster.getLatestQuote("TCS.NS")).isNotNull();
    }
}
