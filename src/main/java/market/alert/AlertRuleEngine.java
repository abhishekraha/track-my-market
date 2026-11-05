package market.alert;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import market.sbe.MarketTickDecoder;
import market.sbe.MessageHeaderDecoder;
import org.agrona.DirectBuffer;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * High-performance alert evaluation engine that evaluates incoming ticks against active rules,
 * detects genuine threshold crossings, manages one-shot and recurring cooldown lifecycles,
 * dispatches Telegram alerts on Virtual Threads, and notifies registered listeners.
 */
public class AlertRuleEngine implements FragmentHandler {

    private final AlertRepository repository;
    private final TelegramDispatcher telegramDispatcher;
    private final CopyOnWriteArrayList<AlertTriggerListener> listeners = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, Double> lastKnownPrices = new ConcurrentHashMap<>();

    // SBE decoders for zero-allocation Aeron fragment polling
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final MarketTickDecoder tickDecoder = new MarketTickDecoder();

    public AlertRuleEngine(AlertRepository repository) {
        this(repository, new TelegramDispatcher());
    }

    public AlertRuleEngine(AlertRepository repository, TelegramDispatcher telegramDispatcher) {
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.telegramDispatcher = telegramDispatcher;
    }

    public AlertRuleEngine(AlertRepository repository, TelegramDispatcher telegramDispatcher, AlertTriggerListener listener) {
        this(repository, telegramDispatcher);
        if (listener != null) {
            this.listeners.add(listener);
        }
    }

    /**
     * Registers a callback listener to receive triggered alert events.
     *
     * @param listener callback listener
     */
    public void registerListener(AlertTriggerListener listener) {
        if (listener != null) {
            listeners.addIfAbsent(listener);
        }
    }

    /**
     * Unregisters a callback listener.
     *
     * @param listener callback listener
     */
    public void unregisterListener(AlertTriggerListener listener) {
        if (listener != null) {
            listeners.remove(listener);
        }
    }

    /**
     * Returns the cached last known price for a symbol, or null if no tick has been received.
     *
     * @param symbol ticker symbol
     * @return last known price or null
     */
    public Double getLastKnownPrice(String symbol) {
        if (symbol == null) {
            return null;
        }
        return lastKnownPrices.get(symbol.trim().toUpperCase(Locale.ROOT));
    }

    /**
     * Returns an unmodifiable snapshot view of all cached prices.
     *
     * @return map of symbol to last known price
     */
    public Map<String, Double> getLastKnownPrices() {
        return Collections.unmodifiableMap(lastKnownPrices);
    }

    /**
     * Evaluates active alert rules against an incoming market tick.
     *
     * @param symbol           market ticker symbol
     * @param price            current tick price
     * @param timestampEpochMs tick timestamp in epoch milliseconds
     */
    public synchronized void onTick(String symbol, double price, long timestampEpochMs) {
        if (symbol == null || symbol.isBlank() || Double.isNaN(price) || Double.isInfinite(price)) {
            return;
        }

        String sym = symbol.trim().toUpperCase(Locale.ROOT);
        Double prevPrice = lastKnownPrices.get(sym);

        List<AlertRule> activeRules = repository.getActiveRules();
        for (AlertRule rule : activeRules) {
            if (!rule.symbol().equalsIgnoreCase(sym)) {
                continue;
            }

            boolean breached = switch (rule.condition()) {
                case ABOVE -> price >= rule.targetPrice();
                case BELOW -> price <= rule.targetPrice();
            };

            if (!breached) {
                continue;
            }

            boolean crossed;
            if (prevPrice != null) {
                crossed = switch (rule.condition()) {
                    case ABOVE -> prevPrice < rule.targetPrice();
                    case BELOW -> prevPrice > rule.targetPrice();
                };
            } else {
                crossed = switch (rule.condition()) {
                    case ABOVE -> rule.referencePrice() < rule.targetPrice();
                    case BELOW -> rule.referencePrice() > rule.targetPrice();
                };
            }

            boolean shouldTrigger = false;
            AlertRule triggeredRule = null;

            if (rule.triggerMode() == TriggerMode.ONE_SHOT) {
                if (crossed) {
                    shouldTrigger = true;
                    triggeredRule = rule.withStatusAndTriggeredAt(AlertStatus.TRIGGERED, timestampEpochMs);
                }
            } else if (rule.triggerMode() == TriggerMode.RECURRING) {
                if (rule.lastTriggeredAtEpochMs() == 0L) {
                    if (crossed) {
                        shouldTrigger = true;
                        triggeredRule = rule.withLastTriggeredAt(timestampEpochMs);
                    }
                } else {
                    long cooldownMs = (long) rule.cooldownMinutes() * 60_000L;
                    if (timestampEpochMs - rule.lastTriggeredAtEpochMs() >= cooldownMs) {
                        shouldTrigger = true;
                        triggeredRule = rule.withLastTriggeredAt(timestampEpochMs);
                    }
                }
            }

            if (shouldTrigger && triggeredRule != null) {
                repository.updateRule(triggeredRule);

                String message = String.format(Locale.US,
                    "%s price %s %.2f (current: %.2f)",
                    rule.symbol(),
                    rule.condition() == AlertCondition.ABOVE ? "breached above" : "breached below",
                    rule.targetPrice(),
                    price
                );

                AlertEvent event = new AlertEvent(
                    rule.id(),
                    rule.symbol(),
                    rule.condition(),
                    rule.targetPrice(),
                    price,
                    timestampEpochMs,
                    message
                );

                repository.recordEvent(event);

                // Notify in-process listeners (e.g. SSE broadcaster)
                for (AlertTriggerListener listener : listeners) {
                    try {
                        listener.onTrigger(triggeredRule, event);
                    } catch (Exception e) {
                        System.err.println("[AlertRuleEngine] Listener notification failed: " + e.getMessage());
                    }
                }

                // Virtual thread dispatch for Telegram
                if (triggeredRule.notifyTelegram() && telegramDispatcher != null) {
                    TelegramConfig tgConfig = repository.getTelegramConfig();
                    if (tgConfig != null && tgConfig.enabled() && tgConfig.isConfigured()) {
                        AlertRule finalRule = triggeredRule;
                        Thread.ofVirtual().start(() -> {
                            telegramDispatcher.sendAlert(
                                tgConfig.botToken(),
                                tgConfig.chatId(),
                                finalRule,
                                event
                            );
                        });
                    }
                }
            }
        }

        lastKnownPrices.put(sym, price);
    }

    /**
     * Aeron FragmentHandler implementation for zero-copy off-heap polling.
     */
    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        if (buffer == null || length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }

        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.templateId() != MarketTickDecoder.TEMPLATE_ID) {
            return;
        }

        tickDecoder.wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            headerDecoder.blockLength(),
            headerDecoder.version()
        );

        String symbol = tickDecoder.symbol().value().trim();
        double price = tickDecoder.price() / 10_000.0;
        long timestampEpochMs = tickDecoder.quoteTimestampNs() / 1_000_000L;

        onTick(symbol, price, timestampEpochMs);
    }
}
