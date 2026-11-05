package market.alert;

/**
 * Operational lifecycle status of an alert rule.
 */
public enum AlertStatus {
    /**
     * Rule is actively evaluated against incoming market ticks.
     */
    ACTIVE,

    /**
     * One-shot rule has met its condition and is inactive until re-armed.
     */
    TRIGGERED,

    /**
     * Rule is paused/disabled by the user.
     */
    DISABLED
}
