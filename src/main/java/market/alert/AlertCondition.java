package market.alert;

/**
 * Price threshold trigger condition.
 */
public enum AlertCondition {
    /**
     * Price crosses or reaches above target price (price >= targetPrice).
     */
    ABOVE,

    /**
     * Price crosses or reaches below target price (price <= targetPrice).
     */
    BELOW
}
