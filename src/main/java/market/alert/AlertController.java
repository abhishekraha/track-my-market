package market.alert;

import market.provider.MarketDataProvider;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * REST controller implementing business logic and JSON handling for:
 * - Querying alert rules and history logs (/api/alerts)
 * - Creating new threshold alert rules with auto-subscription (/api/alerts/create)
 * - Toggling rule status (Pause / Resume / Re-arm) (/api/alerts/toggle)
 * - Deleting alert rules (/api/alerts/delete)
 * - Querying and updating Telegram bot configurations (/api/alerts/telegram/config)
 * - Testing Telegram bot dispatch connectivity (/api/alerts/telegram/test)
 */
public class AlertController {

    private static final Pattern VALID_SYMBOL_PATTERN = Pattern.compile("^[A-Z0-9_.-]+$");

    private final AlertRepository repository;
    private final AlertRuleEngine alertEngine;
    private final TelegramDispatcher telegramDispatcher;
    private final MarketDataProvider provider;

    public AlertController(
        AlertRepository repository,
        AlertRuleEngine alertEngine,
        TelegramDispatcher telegramDispatcher,
        MarketDataProvider provider
    ) {
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.alertEngine = Objects.requireNonNull(alertEngine, "alertEngine must not be null");
        this.telegramDispatcher = (telegramDispatcher != null) ? telegramDispatcher : new TelegramDispatcher();
        this.provider = provider;
    }

    public AlertRepository getRepository() {
        return repository;
    }

    public AlertRuleEngine getAlertEngine() {
        return alertEngine;
    }

    public TelegramDispatcher getTelegramDispatcher() {
        return telegramDispatcher;
    }

    /**
     * Handles GET /api/alerts
     *
     * @return JSON response containing rules, history, and telegram config
     */
    public String handleGetAlertsJson() {
        return AlertJsonCodec.serializeAlertsResponse(
            repository.getAllRules(),
            repository.getHistory(),
            repository.getTelegramConfig()
        );
    }

    /**
     * Handles POST /api/alerts/create
     *
     * @param bodyJson JSON request payload
     * @return JSON response containing created rule
     */
    public String handleCreateAlertJson(String bodyJson) {
        if (bodyJson == null || bodyJson.isBlank()) {
            throw new IllegalArgumentException("Request body must not be empty");
        }

        Map<String, Object> map = AlertJsonCodec.parseJsonObject(bodyJson);
        if (map == null) {
            throw new IllegalArgumentException("Invalid JSON payload");
        }

        String rawSymbol = AlertJsonCodec.getString(map, "symbol", null);
        if (rawSymbol == null || rawSymbol.isBlank()) {
            throw new IllegalArgumentException("Missing required parameter: symbol");
        }
        String symbol = rawSymbol.trim().toUpperCase(Locale.ROOT);
        if (!VALID_SYMBOL_PATTERN.matcher(symbol).matches()) {
            throw new IllegalArgumentException("Invalid symbol format: " + symbol);
        }

        String conditionStr = AlertJsonCodec.getString(map, "condition", "ABOVE").trim().toUpperCase(Locale.ROOT);
        AlertCondition condition;
        try {
            condition = AlertCondition.valueOf(conditionStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid condition: " + conditionStr + ". Must be ABOVE or BELOW.");
        }

        double targetPrice = AlertJsonCodec.getDouble(map, "targetPrice", 0.0);
        if (targetPrice <= 0.0 || Double.isNaN(targetPrice) || Double.isInfinite(targetPrice)) {
            throw new IllegalArgumentException("Target price must be a positive number");
        }

        String triggerModeStr = AlertJsonCodec.getString(map, "triggerMode", "ONE_SHOT").trim().toUpperCase(Locale.ROOT);
        TriggerMode triggerMode;
        try {
            triggerMode = TriggerMode.valueOf(triggerModeStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid triggerMode: " + triggerModeStr + ". Must be ONE_SHOT or RECURRING.");
        }

        int cooldownMinutes = AlertJsonCodec.getInt(map, "cooldownMinutes", 15);
        if (cooldownMinutes < 1) {
            cooldownMinutes = 15;
        }

        boolean notifyTelegram = AlertJsonCodec.getBoolean(map, "notifyTelegram", true);
        boolean notifyUi = AlertJsonCodec.getBoolean(map, "notifyUi", true);

        // Determine reference price to prevent immediate false positive if price already breached
        Double lastPrice = alertEngine.getLastKnownPrice(symbol);
        double referencePrice = (lastPrice != null) ? lastPrice : targetPrice;

        String id = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();

        AlertRule rule = new AlertRule(
            id,
            symbol,
            condition,
            targetPrice,
            triggerMode,
            cooldownMinutes,
            notifyTelegram,
            notifyUi,
            AlertStatus.ACTIVE,
            referencePrice,
            0L,
            now
        );

        repository.addRule(rule);

        return "{\"success\":true,\"rule\":" + AlertJsonCodec.serializeRule(rule) + "}";
    }

    /**
     * Handles POST /api/alerts/toggle
     *
     * @param bodyJson JSON request payload
     * @return JSON response containing updated rule
     */
    public String handleToggleAlertJson(String bodyJson) {
        if (bodyJson == null || bodyJson.isBlank()) {
            throw new IllegalArgumentException("Request body must not be empty");
        }

        Map<String, Object> map = AlertJsonCodec.parseJsonObject(bodyJson);
        if (map == null) {
            throw new IllegalArgumentException("Invalid JSON payload");
        }

        String id = AlertJsonCodec.getString(map, "id", null);
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Missing required parameter: id");
        }

        AlertRule existing = repository.getRule(id);
        if (existing == null) {
            throw new IllegalArgumentException("Alert rule not found: " + id);
        }

        boolean active = AlertJsonCodec.getBoolean(map, "active", true);
        if (map.containsKey("status")) {
            String statusStr = AlertJsonCodec.getString(map, "status", "ACTIVE").trim().toUpperCase(Locale.ROOT);
            active = !"DISABLED".equalsIgnoreCase(statusStr);
        }

        AlertRule updated;
        if (active) {
            Double currentPrice = alertEngine.getLastKnownPrice(existing.symbol());
            double refPrice = (currentPrice != null) ? currentPrice : existing.referencePrice();
            updated = repository.rearmRule(id, refPrice);
        } else {
            updated = repository.updateStatus(id, AlertStatus.DISABLED);
        }

        if (updated == null) {
            throw new IllegalArgumentException("Failed to update alert rule: " + id);
        }

        return "{\"success\":true,\"rule\":" + AlertJsonCodec.serializeRule(updated) + "}";
    }

    /**
     * Handles POST /api/alerts/delete
     *
     * @param bodyJson JSON request payload
     * @return JSON response confirming deletion
     */
    public String handleDeleteAlertJson(String bodyJson) {
        if (bodyJson == null || bodyJson.isBlank()) {
            throw new IllegalArgumentException("Request body must not be empty");
        }

        Map<String, Object> map = AlertJsonCodec.parseJsonObject(bodyJson);
        if (map == null) {
            throw new IllegalArgumentException("Invalid JSON payload");
        }

        String id = AlertJsonCodec.getString(map, "id", null);
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Missing required parameter: id");
        }

        boolean deleted = repository.deleteRule(id);
        return "{\"success\":" + deleted + "}";
    }

    /**
     * Handles GET /api/alerts/telegram/config
     *
     * @return JSON response with masked telegram config
     */
    public String handleGetTelegramConfigJson() {
        return AlertJsonCodec.serializeTelegramConfig(repository.getTelegramConfig());
    }

    /**
     * Handles POST /api/alerts/telegram/config
     *
     * @param bodyJson JSON request payload
     * @return JSON response with updated masked config
     */
    public String handleSaveTelegramConfigJson(String bodyJson) {
        if (bodyJson == null || bodyJson.isBlank()) {
            throw new IllegalArgumentException("Request body must not be empty");
        }

        Map<String, Object> map = AlertJsonCodec.parseJsonObject(bodyJson);
        if (map == null) {
            throw new IllegalArgumentException("Invalid JSON payload");
        }

        TelegramConfig current = repository.getTelegramConfig();

        String rawToken = AlertJsonCodec.getString(map, "botToken", "");
        String botToken = rawToken.isBlank() ? (current != null ? current.botToken() : "") : rawToken.trim();

        String rawChatId = AlertJsonCodec.getString(map, "chatId", "");
        String chatId = rawChatId.isBlank() ? (current != null ? current.chatId() : "") : rawChatId.trim();

        boolean enabled = AlertJsonCodec.getBoolean(map, "enabled", current != null && current.enabled());

        TelegramConfig updated = new TelegramConfig(botToken, chatId, enabled);
        repository.updateTelegramConfig(updated);

        return "{\"success\":true,\"config\":" + AlertJsonCodec.serializeTelegramConfig(updated) + "}";
    }

    /**
     * Handles POST /api/alerts/telegram/test
     *
     * @param bodyJson JSON request payload
     * @return JSON response with delivery status and message
     */
    public String handleTestTelegramJson(String bodyJson) {
        String botToken = "";
        String chatId = "";

        if (bodyJson != null && !bodyJson.isBlank()) {
            Map<String, Object> map = AlertJsonCodec.parseJsonObject(bodyJson);
            if (map != null) {
                botToken = AlertJsonCodec.getString(map, "botToken", "").trim();
                chatId = AlertJsonCodec.getString(map, "chatId", "").trim();
            }
        }

        TelegramConfig current = repository.getTelegramConfig();
        if (botToken.isBlank() && current != null) {
            botToken = current.botToken();
        }
        if (chatId.isBlank() && current != null) {
            chatId = current.chatId();
        }

        if (botToken.isBlank() || chatId.isBlank()) {
            return "{\"success\":false,\"message\":\"Telegram Bot Token and Chat ID must be configured before testing\"}";
        }

        boolean success = telegramDispatcher.testConnection(botToken, chatId);
        if (success) {
            return "{\"success\":true,\"message\":\"Test ping delivered successfully to Telegram chat " + chatId + "\"}";
        } else {
            return "{\"success\":false,\"message\":\"Failed to deliver Telegram test message. Verify your Bot Token and Chat ID.\"}";
        }
    }
}
