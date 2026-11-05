package market.alert;

/**
 * Telegram notification dispatch configuration.
 *
 * @param botToken Telegram bot token (e.g. "123456789:ABCdefGHIjklMNOpqrSTUvwxYZ")
 * @param chatId   target Telegram chat or channel ID (e.g. "-100123456789" or "987654321")
 * @param enabled  whether Telegram notification dispatch is active
 */
public record TelegramConfig(
    String botToken,
    String chatId,
    boolean enabled
) {

    public TelegramConfig {
        botToken = botToken == null ? "" : botToken.trim();
        chatId = chatId == null ? "" : chatId.trim();
    }

    /**
     * Checks if this configuration contains non-empty credentials.
     *
     * @return true if both botToken and chatId are present
     */
    public boolean isConfigured() {
        return !botToken.isBlank() && !chatId.isBlank();
    }

    /**
     * Returns a copy with masked botToken for safe presentation in UI and logs.
     *
     * @return masked TelegramConfig
     */
    public TelegramConfig masked() {
        if (botToken.isBlank()) {
            return new TelegramConfig("", chatId, enabled);
        }
        if (botToken.length() <= 4) {
            return new TelegramConfig("****", chatId, enabled);
        }
        return new TelegramConfig("****" + botToken.substring(botToken.length() - 4), chatId, enabled);
    }
}
