package market.alert;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;

/**
 * Dispatches alert notifications to Telegram channels and chats via the Telegram Bot API.
 */
public class TelegramDispatcher {

    public static final String DEFAULT_BASE_URL = "https://api.telegram.org";
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);
    private static final DateTimeFormatter TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withLocale(Locale.US);

    private final HttpClient httpClient;
    private final String baseUrl;

    public TelegramDispatcher() {
        this(HttpClient.newBuilder().connectTimeout(DEFAULT_TIMEOUT).build(), DEFAULT_BASE_URL);
    }

    public TelegramDispatcher(HttpClient httpClient, String baseUrl) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        this.baseUrl = (baseUrl != null && !baseUrl.isBlank()) ? baseUrl : DEFAULT_BASE_URL;
    }

    /**
     * Formats an AlertRule and AlertEvent into an expressive Telegram Markdown message.
     *
     * @param rule  the triggered alert rule
     * @param event the fired alert event
     * @return formatted markdown text
     */
    public String formatAlertMessage(AlertRule rule, AlertEvent event) {
        String sym = rule.symbol();
        String currency = (sym.endsWith(".NS") || sym.endsWith(".BO")) ? "₹" : "";
        String timeStr = TIME_FORMATTER.format(
            Instant.ofEpochMilli(event.timestampEpochMs()).atZone(ZoneId.systemDefault())
        );

        String modeStr = (rule.triggerMode() == TriggerMode.ONE_SHOT)
            ? "One-Shot"
            : String.format("Recurring (Cooldown: %dm)", rule.cooldownMinutes());

        return String.format(Locale.US,
            "🚨 *MARKET ALERT TRIGGERED*\n" +
            "---------------------------------\n" +
            "📈 *Symbol:* `%s`\n" +
            "🎯 *Condition:* Price %s %s%,.2f\n" +
            "⚡ *Current Price:* *%s%,.2f*\n" +
            "🕒 *Time:* %s\n" +
            "🔔 *Mode:* %s",
            sym,
            rule.condition().name(),
            currency,
            rule.targetPrice(),
            currency,
            event.triggerPrice(),
            timeStr,
            modeStr
        );
    }

    /**
     * Sends an alert notification message to Telegram.
     *
     * @param botToken Telegram bot token
     * @param chatId   target chat or channel ID
     * @param rule     the triggered alert rule
     * @param event    the fired alert event
     * @return true if Telegram API returned 2xx HTTP status, false otherwise
     */
    public boolean sendAlert(String botToken, String chatId, AlertRule rule, AlertEvent event) {
        String message = formatAlertMessage(rule, event);
        return sendMessage(botToken, chatId, message);
    }

    /**
     * Sends a raw message to Telegram Bot API sendMessage endpoint.
     *
     * @param botToken Telegram bot token
     * @param chatId   target chat or channel ID
     * @param text     message text in Markdown format
     * @return true if accepted by Telegram API (2xx status), false otherwise
     */
    public boolean sendMessage(String botToken, String chatId, String text) {
        if (botToken == null || botToken.isBlank() || chatId == null || chatId.isBlank()) {
            return false;
        }

        String url = String.format("%s/bot%s/sendMessage", baseUrl, botToken.trim());
        String payload = String.format(
            "{\"chat_id\":\"%s\",\"text\":\"%s\",\"parse_mode\":\"Markdown\"}",
            escapeJson(chatId.trim()),
            escapeJson(text)
        );

        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(DEFAULT_TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() >= 200 && response.statusCode() < 300;
        } catch (Exception e) {
            System.err.println("[TelegramDispatcher] Failed to dispatch Telegram message: " + e.getMessage());
            return false;
        }
    }

    /**
     * Tests connectivity and credentials by sending a verification ping to Telegram.
     *
     * @param botToken Telegram bot token
     * @param chatId   target chat or channel ID
     * @return true if the test message was successfully accepted, false otherwise
     */
    public boolean testConnection(String botToken, String chatId) {
        String timeStr = TIME_FORMATTER.format(Instant.now().atZone(ZoneId.systemDefault()));
        String testMessage = String.format(
            "🤖 *Track My Market Alert Test*\n" +
            "---------------------------------\n" +
            "Status: Connected successfully ✅\n" +
            "Time: %s\n" +
            "Channel: Verified and ready for alerts.",
            timeStr
        );
        return sendMessage(botToken, chatId, testMessage);
    }

    private static String escapeJson(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
