package market.alert;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lightweight, zero-dependency JSON serializer and parser for Alert domain models.
 */
public final class AlertJsonCodec {

    private AlertJsonCodec() {
    }

    /**
     * Serializes a single AlertRule into JSON.
     */
    public static String serializeRule(AlertRule r) {
        if (r == null) {
            return "null";
        }
        return String.format(java.util.Locale.ROOT,
            "{\"id\":\"%s\",\"symbol\":\"%s\",\"condition\":\"%s\",\"targetPrice\":%.4f,\"triggerMode\":\"%s\",\"cooldownMinutes\":%d,\"notifyTelegram\":%b,\"notifyUi\":%b,\"status\":\"%s\",\"referencePrice\":%.4f,\"lastTriggeredAtEpochMs\":%d,\"createdAtEpochMs\":%d}",
            escapeJson(r.id()),
            escapeJson(r.symbol()),
            r.condition().name(),
            r.targetPrice(),
            r.triggerMode().name(),
            r.cooldownMinutes(),
            r.notifyTelegram(),
            r.notifyUi(),
            r.status().name(),
            r.referencePrice(),
            r.lastTriggeredAtEpochMs(),
            r.createdAtEpochMs()
        );
    }

    /**
     * Serializes an AlertEvent into JSON, incorporating rule UI flags if available.
     */
    public static String serializeEvent(AlertRule r, AlertEvent e) {
        if (e == null) {
            return "null";
        }
        boolean notifyUi = (r != null) ? r.notifyUi() : true;
        return String.format(java.util.Locale.ROOT,
            "{\"alertId\":\"%s\",\"symbol\":\"%s\",\"condition\":\"%s\",\"targetPrice\":%.4f,\"triggerPrice\":%.4f,\"timestampEpochMs\":%d,\"message\":\"%s\",\"notifyUi\":%b}",
            escapeJson(e.alertId()),
            escapeJson(e.symbol()),
            e.condition().name(),
            e.targetPrice(),
            e.triggerPrice(),
            e.timestampEpochMs(),
            escapeJson(e.message()),
            notifyUi
        );
    }

    /**
     * Serializes TelegramConfig into JSON with masked token for UI display.
     */
    public static String serializeTelegramConfig(TelegramConfig c) {
        if (c == null) {
            return "{\"configured\":false,\"maskedToken\":\"\",\"chatId\":\"\",\"enabled\":false}";
        }
        TelegramConfig masked = c.masked();
        return String.format(java.util.Locale.ROOT,
            "{\"configured\":%b,\"maskedToken\":\"%s\",\"chatId\":\"%s\",\"enabled\":%b}",
            c.isConfigured(),
            escapeJson(masked.botToken()),
            escapeJson(c.chatId()),
            c.enabled()
        );
    }

    /**
     * Serializes alerts list, history, and telegram config into full API response JSON.
     */
    public static String serializeAlertsResponse(List<AlertRule> rules, List<AlertEvent> history, TelegramConfig telegramConfig) {
        StringBuilder sb = new StringBuilder(1024);
        sb.append("{\"rules\":[");
        if (rules != null) {
            for (int i = 0; i < rules.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(serializeRule(rules.get(i)));
            }
        }
        sb.append("],\"history\":[");
        if (history != null) {
            for (int i = 0; i < history.size(); i++) {
                if (i > 0) sb.append(",");
                AlertEvent e = history.get(i);
                sb.append(String.format(java.util.Locale.ROOT,
                    "{\"alertId\":\"%s\",\"symbol\":\"%s\",\"condition\":\"%s\",\"targetPrice\":%.4f,\"triggerPrice\":%.4f,\"timestampEpochMs\":%d,\"message\":\"%s\"}",
                    escapeJson(e.alertId()),
                    escapeJson(e.symbol()),
                    e.condition().name(),
                    e.targetPrice(),
                    e.triggerPrice(),
                    e.timestampEpochMs(),
                    escapeJson(e.message())
                ));
            }
        }
        sb.append("],\"telegram\":");
        sb.append(serializeTelegramConfig(telegramConfig));
        sb.append("}");
        return sb.toString();
    }

    /**
     * Parses arbitrary JSON into parsed Java objects (Map, List, String, Number, Boolean, null).
     */
    public static Object parseJson(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        return new MiniParser(json).parseValue();
    }

    /**
     * Parses a JSON string into a Map of key-value pairs.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseJsonObject(String json) {
        Object val = parseJson(json);
        if (val instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return null;
    }

    /**
     * Serializes repository state to formatted JSON.
     */
    public static String serialize(TelegramConfig telegramConfig, List<AlertRule> rules, List<AlertEvent> history) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");

        // Telegram
        if (telegramConfig != null) {
            sb.append("  \"telegram\": {\n");
            sb.append("    \"botToken\": \"").append(escapeJson(telegramConfig.botToken())).append("\",\n");
            sb.append("    \"chatId\": \"").append(escapeJson(telegramConfig.chatId())).append("\",\n");
            sb.append("    \"enabled\": ").append(telegramConfig.enabled()).append("\n");
            sb.append("  },\n");
        } else {
            sb.append("  \"telegram\": null,\n");
        }

        // Rules
        sb.append("  \"rules\": [\n");
        if (rules != null) {
            for (int i = 0; i < rules.size(); i++) {
                AlertRule r = rules.get(i);
                sb.append("    {\n");
                sb.append("      \"id\": \"").append(escapeJson(r.id())).append("\",\n");
                sb.append("      \"symbol\": \"").append(escapeJson(r.symbol())).append("\",\n");
                sb.append("      \"condition\": \"").append(r.condition().name()).append("\",\n");
                sb.append("      \"targetPrice\": ").append(r.targetPrice()).append(",\n");
                sb.append("      \"triggerMode\": \"").append(r.triggerMode().name()).append("\",\n");
                sb.append("      \"cooldownMinutes\": ").append(r.cooldownMinutes()).append(",\n");
                sb.append("      \"notifyTelegram\": ").append(r.notifyTelegram()).append(",\n");
                sb.append("      \"notifyUi\": ").append(r.notifyUi()).append(",\n");
                sb.append("      \"status\": \"").append(r.status().name()).append("\",\n");
                sb.append("      \"referencePrice\": ").append(r.referencePrice()).append(",\n");
                sb.append("      \"lastTriggeredAtEpochMs\": ").append(r.lastTriggeredAtEpochMs()).append(",\n");
                sb.append("      \"createdAtEpochMs\": ").append(r.createdAtEpochMs()).append("\n");
                sb.append("    }").append(i < rules.size() - 1 ? "," : "").append("\n");
            }
        }
        sb.append("  ],\n");

        // History
        sb.append("  \"history\": [\n");
        if (history != null) {
            for (int i = 0; i < history.size(); i++) {
                AlertEvent e = history.get(i);
                sb.append("    {\n");
                sb.append("      \"alertId\": \"").append(escapeJson(e.alertId())).append("\",\n");
                sb.append("      \"symbol\": \"").append(escapeJson(e.symbol())).append("\",\n");
                sb.append("      \"condition\": \"").append(e.condition().name()).append("\",\n");
                sb.append("      \"targetPrice\": ").append(e.targetPrice()).append(",\n");
                sb.append("      \"triggerPrice\": ").append(e.triggerPrice()).append(",\n");
                sb.append("      \"timestampEpochMs\": ").append(e.timestampEpochMs()).append(",\n");
                sb.append("      \"message\": \"").append(escapeJson(e.message())).append("\"\n");
                sb.append("    }").append(i < history.size() - 1 ? "," : "").append("\n");
            }
        }
        sb.append("  ]\n");

        sb.append("}\n");
        return sb.toString();
    }

    /**
     * Parses JSON string into a structured Snapshot containing TelegramConfig, Rules, and History.
     */
    @SuppressWarnings("unchecked")
    public static Snapshot deserialize(String json) {
        if (json == null || json.isBlank()) {
            return new Snapshot(null, List.of(), List.of());
        }

        Object parsed = new MiniParser(json).parseValue();
        if (!(parsed instanceof Map<?, ?> root)) {
            return new Snapshot(null, List.of(), List.of());
        }

        // Telegram
        TelegramConfig tc = null;
        Object tgObj = root.get("telegram");
        if (tgObj instanceof Map<?, ?> tgMap) {
            String token = getString(tgMap, "botToken", "");
            String chatId = getString(tgMap, "chatId", "");
            boolean enabled = getBoolean(tgMap, "enabled", false);
            tc = new TelegramConfig(token, chatId, enabled);
        }

        // Rules
        List<AlertRule> rules = new ArrayList<>();
        Object rulesObj = root.get("rules");
        if (rulesObj instanceof List<?> rulesList) {
            for (Object item : rulesList) {
                if (item instanceof Map<?, ?> rMap) {
                    try {
                        String id = getString(rMap, "id", null);
                        String symbol = getString(rMap, "symbol", null);
                        String conditionStr = getString(rMap, "condition", "ABOVE");
                        AlertCondition condition = AlertCondition.valueOf(conditionStr);
                        double targetPrice = getDouble(rMap, "targetPrice", 0.0);
                        String triggerModeStr = getString(rMap, "triggerMode", "ONE_SHOT");
                        TriggerMode triggerMode = TriggerMode.valueOf(triggerModeStr);
                        int cooldown = getInt(rMap, "cooldownMinutes", 15);
                        boolean notifyTelegram = getBoolean(rMap, "notifyTelegram", true);
                        boolean notifyUi = getBoolean(rMap, "notifyUi", true);
                        String statusStr = getString(rMap, "status", "ACTIVE");
                        AlertStatus status = AlertStatus.valueOf(statusStr);
                        double referencePrice = getDouble(rMap, "referencePrice", targetPrice);
                        long lastTriggeredAt = getLong(rMap, "lastTriggeredAtEpochMs", 0L);
                        long createdAt = getLong(rMap, "createdAtEpochMs", System.currentTimeMillis());

                        if (id != null && symbol != null) {
                            rules.add(new AlertRule(
                                id,
                                symbol,
                                condition,
                                targetPrice,
                                triggerMode,
                                cooldown,
                                notifyTelegram,
                                notifyUi,
                                status,
                                referencePrice,
                                lastTriggeredAt,
                                createdAt
                            ));
                        }
                    } catch (Exception ignored) {
                        // Skip malformed individual rule
                    }
                }
            }
        }

        // History
        List<AlertEvent> history = new ArrayList<>();
        Object historyObj = root.get("history");
        if (historyObj instanceof List<?> histList) {
            for (Object item : histList) {
                if (item instanceof Map<?, ?> hMap) {
                    try {
                        String alertId = getString(hMap, "alertId", "");
                        String symbol = getString(hMap, "symbol", "");
                        String conditionStr = getString(hMap, "condition", "ABOVE");
                        AlertCondition condition = AlertCondition.valueOf(conditionStr);
                        double targetPrice = getDouble(hMap, "targetPrice", 0.0);
                        double triggerPrice = getDouble(hMap, "triggerPrice", 0.0);
                        long timestampEpochMs = getLong(hMap, "timestampEpochMs", 0L);
                        String message = getString(hMap, "message", "");

                        if (!alertId.isEmpty() && !symbol.isEmpty()) {
                            history.add(new AlertEvent(
                                alertId,
                                symbol,
                                condition,
                                targetPrice,
                                triggerPrice,
                                timestampEpochMs,
                                message
                            ));
                        }
                    } catch (Exception ignored) {
                        // Skip malformed history record
                    }
                }
            }
        }

        return new Snapshot(tc, rules, history);
    }

    public record Snapshot(
        TelegramConfig telegramConfig,
        List<AlertRule> rules,
        List<AlertEvent> history
    ) {}

    public static String escapeJson(String s) {
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

    public static String getString(Map<?, ?> map, String key, String defaultVal) {
        Object val = map.get(key);
        return val != null ? val.toString() : defaultVal;
    }

    public static double getDouble(Map<?, ?> map, String key, double defaultVal) {
        Object val = map.get(key);
        if (val instanceof Number n) {
            return n.doubleValue();
        }
        if (val instanceof String s) {
            try {
                return Double.parseDouble(s);
            } catch (NumberFormatException ignored) {}
        }
        return defaultVal;
    }

    public static int getInt(Map<?, ?> map, String key, int defaultVal) {
        Object val = map.get(key);
        if (val instanceof Number n) {
            return n.intValue();
        }
        if (val instanceof String s) {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException ignored) {}
        }
        return defaultVal;
    }

    public static long getLong(Map<?, ?> map, String key, long defaultVal) {
        Object val = map.get(key);
        if (val instanceof Number n) {
            return n.longValue();
        }
        if (val instanceof String s) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException ignored) {}
        }
        return defaultVal;
    }

    public static boolean getBoolean(Map<?, ?> map, String key, boolean defaultVal) {
        Object val = map.get(key);
        if (val instanceof Boolean b) {
            return b;
        }
        if (val instanceof String s) {
            return Boolean.parseBoolean(s);
        }
        return defaultVal;
    }

    private static class MiniParser {
        private final String src;
        private int pos = 0;

        MiniParser(String src) {
            this.src = src;
        }

        Object parseValue() {
            skipWhitespace();
            if (pos >= src.length()) {
                return null;
            }
            char c = src.charAt(pos);
            if (c == '{') {
                return parseObject();
            } else if (c == '[') {
                return parseArray();
            } else if (c == '"') {
                return parseString();
            } else if (c == 't' || c == 'f') {
                return parseBoolean();
            } else if (c == 'n') {
                return parseNull();
            } else if (c == '-' || Character.isDigit(c)) {
                return parseNumber();
            }
            return null;
        }

        private Map<String, Object> parseObject() {
            Map<String, Object> map = new LinkedHashMap<>();
            pos++; // skip '{'
            skipWhitespace();
            if (pos < src.length() && src.charAt(pos) == '}') {
                pos++;
                return map;
            }
            while (pos < src.length()) {
                skipWhitespace();
                if (pos >= src.length() || src.charAt(pos) != '"') {
                    break;
                }
                String key = parseString();
                skipWhitespace();
                if (pos < src.length() && src.charAt(pos) == ':') {
                    pos++;
                }
                Object val = parseValue();
                map.put(key, val);
                skipWhitespace();
                if (pos < src.length() && src.charAt(pos) == ',') {
                    pos++;
                } else if (pos < src.length() && src.charAt(pos) == '}') {
                    pos++;
                    break;
                } else {
                    break;
                }
            }
            return map;
        }

        private List<Object> parseArray() {
            List<Object> list = new ArrayList<>();
            pos++; // skip '['
            skipWhitespace();
            if (pos < src.length() && src.charAt(pos) == ']') {
                pos++;
                return list;
            }
            while (pos < src.length()) {
                Object val = parseValue();
                list.add(val);
                skipWhitespace();
                if (pos < src.length() && src.charAt(pos) == ',') {
                    pos++;
                } else if (pos < src.length() && src.charAt(pos) == ']') {
                    pos++;
                    break;
                } else {
                    break;
                }
            }
            return list;
        }

        private String parseString() {
            pos++; // skip opening '"'
            StringBuilder sb = new StringBuilder();
            while (pos < src.length()) {
                char c = src.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                } else if (c == '\\' && pos < src.length()) {
                    char esc = src.charAt(pos++);
                    switch (esc) {
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/' -> sb.append('/');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'u' -> {
                            if (pos + 4 <= src.length()) {
                                String hex = src.substring(pos, pos + 4);
                                pos += 4;
                                try {
                                    sb.append((char) Integer.parseInt(hex, 16));
                                } catch (NumberFormatException ignored) {}
                            }
                        }
                        default -> sb.append(esc);
                    }
                } else {
                    sb.append(c);
                }
            }
            return sb.toString();
        }

        private Boolean parseBoolean() {
            if (src.startsWith("true", pos)) {
                pos += 4;
                return Boolean.TRUE;
            } else if (src.startsWith("false", pos)) {
                pos += 5;
                return Boolean.FALSE;
            }
            return null;
        }

        private Object parseNull() {
            if (src.startsWith("null", pos)) {
                pos += 4;
            }
            return null;
        }

        private Number parseNumber() {
            int start = pos;
            if (pos < src.length() && (src.charAt(pos) == '-' || src.charAt(pos) == '+')) {
                pos++;
            }
            boolean isFloating = false;
            while (pos < src.length()) {
                char c = src.charAt(pos);
                if (Character.isDigit(c)) {
                    pos++;
                } else if (c == '.' || c == 'e' || c == 'E') {
                    isFloating = true;
                    pos++;
                    if (pos < src.length() && (src.charAt(pos) == '+' || src.charAt(pos) == '-')) {
                        pos++;
                    }
                } else {
                    break;
                }
            }
            String numStr = src.substring(start, pos);
            try {
                if (isFloating) {
                    return Double.parseDouble(numStr);
                }
                return Long.parseLong(numStr);
            } catch (NumberFormatException e) {
                return 0L;
            }
        }

        private void skipWhitespace() {
            while (pos < src.length()) {
                char c = src.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    pos++;
                } else {
                    break;
                }
            }
        }
    }
}
