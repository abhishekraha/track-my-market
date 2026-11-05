package market.alert;

import java.util.Objects;

/**
 * Historical record of a fired alert event.
 *
 * @param alertId          identifier of the triggered AlertRule
 * @param symbol           market ticker symbol
 * @param condition        threshold condition (ABOVE / BELOW)
 * @param targetPrice      target threshold price
 * @param triggerPrice     actual tick price that breached the threshold
 * @param timestampEpochMs trigger timestamp in epoch milliseconds
 * @param message          human-readable descriptive notification message
 */
public record AlertEvent(
    String alertId,
    String symbol,
    AlertCondition condition,
    double targetPrice,
    double triggerPrice,
    long timestampEpochMs,
    String message
) {

    public AlertEvent {
        Objects.requireNonNull(alertId, "alertId must not be null");
        Objects.requireNonNull(symbol, "symbol must not be null");
        Objects.requireNonNull(condition, "condition must not be null");
        Objects.requireNonNull(message, "message must not be null");
    }
}
