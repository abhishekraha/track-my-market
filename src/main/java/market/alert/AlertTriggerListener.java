package market.alert;

/**
 * Callback listener invoked when an alert rule is triggered.
 */
@FunctionalInterface
public interface AlertTriggerListener {

    /**
     * Called when an alert rule condition is met and triggered.
     *
     * @param rule  the triggered rule (with updated status and timestamps)
     * @param event the generated alert event
     */
    void onTrigger(AlertRule rule, AlertEvent event);
}
