package market.alert;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class TelegramDispatcherTest {

    private HttpServer server;
    private String serverUrl;
    private final AtomicReference<String> receivedBody = new AtomicReference<>();
    private final AtomicReference<String> receivedPath = new AtomicReference<>();
    private int responseCode = 200;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            receivedPath.set(exchange.getRequestURI().toString());
            try (InputStream is = exchange.getRequestBody()) {
                receivedBody.set(new String(is.readAllBytes(), StandardCharsets.UTF_8));
            }
            byte[] resp = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(responseCode, resp.length);
            exchange.getResponseBody().write(resp);
            exchange.close();
        });
        server.start();
        serverUrl = "http://localhost:" + server.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("Should format markdown message correctly for Indian equity")
    void shouldFormatMarkdownMessageForIndianStock() {
        TelegramDispatcher dispatcher = new TelegramDispatcher();
        AlertRule rule = new AlertRule(
            "test-id",
            "RELIANCE.NS",
            AlertCondition.ABOVE,
            3100.0,
            TriggerMode.RECURRING,
            15,
            true,
            true,
            AlertStatus.ACTIVE,
            3050.0,
            0L,
            1000L
        );
        AlertEvent event = new AlertEvent(
            "test-id",
            "RELIANCE.NS",
            AlertCondition.ABOVE,
            3100.0,
            3105.40,
            1762388050000L,
            "RELIANCE.NS breached above 3100.00"
        );

        String message = dispatcher.formatAlertMessage(rule, event);
        assertThat(message).contains("🚨 *MARKET ALERT TRIGGERED*");
        assertThat(message).contains("`RELIANCE.NS`");
        assertThat(message).contains("Price ABOVE ₹3,100.00");
        assertThat(message).contains("*₹3,105.40*");
        assertThat(message).contains("Recurring (Cooldown: 15m)");
    }

    @Test
    @DisplayName("Should successfully dispatch alert to Telegram Bot API endpoint")
    void shouldDispatchAlertToTelegram() {
        TelegramDispatcher dispatcher = new TelegramDispatcher(
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build(),
            serverUrl
        );

        AlertRule rule = new AlertRule(
            "test-id",
            "AAPL",
            AlertCondition.BELOW,
            180.0,
            TriggerMode.ONE_SHOT,
            15,
            true,
            true,
            AlertStatus.ACTIVE,
            185.0,
            0L,
            1000L
        );
        AlertEvent event = new AlertEvent(
            "test-id",
            "AAPL",
            AlertCondition.BELOW,
            180.0,
            179.50,
            System.currentTimeMillis(),
            "AAPL dipped below 180"
        );

        boolean success = dispatcher.sendAlert("12345:TEST_TOKEN", "-100123456789", rule, event);

        assertThat(success).isTrue();
        assertThat(receivedPath.get()).isEqualTo("/bot12345:TEST_TOKEN/sendMessage");
        assertThat(receivedBody.get()).contains("-100123456789");
        assertThat(receivedBody.get()).contains("AAPL");
    }

    @Test
    @DisplayName("Should test connection and return true when endpoint responds 200")
    void shouldTestConnectionSuccessfully() {
        TelegramDispatcher dispatcher = new TelegramDispatcher(
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build(),
            serverUrl
        );

        boolean ok = dispatcher.testConnection("MY_TOKEN", "CHAT_123");
        assertThat(ok).isTrue();
        assertThat(receivedPath.get()).isEqualTo("/botMY_TOKEN/sendMessage");
        assertThat(receivedBody.get()).contains("Track My Market Alert Test");
    }

    @Test
    @DisplayName("Should return false when Telegram API responds with 4xx error")
    void shouldReturnFalseOnHttpError() {
        responseCode = 401; // Unauthorized
        TelegramDispatcher dispatcher = new TelegramDispatcher(
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build(),
            serverUrl
        );

        boolean ok = dispatcher.testConnection("INVALID_TOKEN", "CHAT_123");
        assertThat(ok).isFalse();
    }
}
