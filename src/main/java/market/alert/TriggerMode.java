package market.alert;

/**
 * Trigger evaluation mode for an alert rule.
 */
public enum TriggerMode {
    /**
     * Fires once upon crossing the threshold and deactivates (status becomes TRIGGERED).
     */
    ONE_SHOT,

    /**
     * Fires upon threshold breach and remains active, respecting a configured cooldown duration.
     */
    RECURRING
}
