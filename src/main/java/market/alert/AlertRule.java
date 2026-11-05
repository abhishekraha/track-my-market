package market.alert;

import java.util.Objects;

/**
 * Representation of a rule-based price threshold alert.
 *
 * @param id                    unique identifier (UUID)
 * @param symbol                market ticker symbol (e.g. "RELIANCE.NS", "AAPL")
 * @param condition             threshold condition (ABOVE / BELOW)
 * @param targetPrice           target price threshold
 * @param triggerMode           ONE_SHOT or RECURRING
 * @param cooldownMinutes       cooldown window in minutes when triggerMode is RECURRING
 * @param notifyTelegram        whether to dispatch notifications to Telegram
 * @param notifyUi              whether to dispatch notifications to UI (toast / audio)
 * @param status                current lifecycle status (ACTIVE, TRIGGERED, DISABLED)
 * @param referencePrice        price at creation / re-arm used for crossing detection
 * @param lastTriggeredAtEpochMs timestamp of last trigger in epoch milliseconds (0 if never)
 * @param createdAtEpochMs      creation timestamp in epoch milliseconds
 */
public record AlertRule(
    String id,
    String symbol,
    AlertCondition condition,
    double targetPrice,
    TriggerMode triggerMode,
    int cooldownMinutes,
    boolean notifyTelegram,
    boolean notifyUi,
    AlertStatus status,
    double referencePrice,
    long lastTriggeredAtEpochMs,
    long createdAtEpochMs
) {

    public AlertRule {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(symbol, "symbol must not be null");
        Objects.requireNonNull(condition, "condition must not be null");
        Objects.requireNonNull(triggerMode, "triggerMode must not be null");
        Objects.requireNonNull(status, "status must not be null");
    }

    /**
     * Creates a copy of this rule with an updated status.
     *
     * @param newStatus new alert status
     * @return updated AlertRule
     */
    public AlertRule withStatus(AlertStatus newStatus) {
        return new AlertRule(
            id,
            symbol,
            condition,
            targetPrice,
            triggerMode,
            cooldownMinutes,
            notifyTelegram,
            notifyUi,
            newStatus,
            referencePrice,
            lastTriggeredAtEpochMs,
            createdAtEpochMs
        );
    }

    /**
     * Creates a copy of this rule with an updated lastTriggeredAt timestamp.
     *
     * @param timestampEpochMs trigger timestamp
     * @return updated AlertRule
     */
    public AlertRule withLastTriggeredAt(long timestampEpochMs) {
        return new AlertRule(
            id,
            symbol,
            condition,
            targetPrice,
            triggerMode,
            cooldownMinutes,
            notifyTelegram,
            notifyUi,
            status,
            referencePrice,
            timestampEpochMs,
            createdAtEpochMs
        );
    }

    /**
     * Creates a copy of this rule with updated status and lastTriggeredAt timestamp.
     *
     * @param newStatus        new status
     * @param timestampEpochMs trigger timestamp
     * @return updated AlertRule
     */
    public AlertRule withStatusAndTriggeredAt(AlertStatus newStatus, long timestampEpochMs) {
        return new AlertRule(
            id,
            symbol,
            condition,
            targetPrice,
            triggerMode,
            cooldownMinutes,
            notifyTelegram,
            notifyUi,
            newStatus,
            referencePrice,
            timestampEpochMs,
            createdAtEpochMs
        );
    }

    /**
     * Re-arms this rule with ACTIVE status and resets lastTriggeredAt.
     *
     * @param newReferencePrice current price to prevent immediate re-triggering if already crossed
     * @return re-armed AlertRule
     */
    public AlertRule rearm(double newReferencePrice) {
        return new AlertRule(
            id,
            symbol,
            condition,
            targetPrice,
            triggerMode,
            cooldownMinutes,
            notifyTelegram,
            notifyUi,
            AlertStatus.ACTIVE,
            newReferencePrice,
            0L,
            createdAtEpochMs
        );
    }
}
