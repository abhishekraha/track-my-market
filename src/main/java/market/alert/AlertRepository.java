package market.alert;

import market.provider.MarketDataProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe repository managing alert rules, history logs, and Telegram configuration
 * with asynchronous JSON persistence and atomic file replacement.
 */
public class AlertRepository implements AutoCloseable {

    public static final Path DEFAULT_STORAGE_PATH = Path.of("data", "alerts.json");
    public static final int MAX_HISTORY_SIZE = 50;

    public static final String ENV_TELEGRAM_BOT_TOKEN = "TELEGRAM_BOT_TOKEN";
    public static final String ENV_TELEGRAM_CHAT_ID = "TELEGRAM_CHAT_ID";

    private final Path storagePath;
    private final ConcurrentHashMap<String, AlertRule> rules = new ConcurrentHashMap<>();
    private final Deque<AlertEvent> history = new ConcurrentLinkedDeque<>();
    private final AtomicReference<TelegramConfig> telegramConfig = new AtomicReference<>(null);
    private final MarketDataProvider provider;
    private final ExecutorService persistenceExecutor;

    public AlertRepository() {
        this(DEFAULT_STORAGE_PATH, null);
    }

    public AlertRepository(Path storagePath) {
        this(storagePath, null);
    }

    public AlertRepository(Path storagePath, MarketDataProvider provider) {
        this.storagePath = Objects.requireNonNull(storagePath, "storagePath must not be null");
        this.provider = provider;
        this.persistenceExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = Thread.ofVirtual().unstarted(r);
            t.setName("AlertRepo-Persistence");
            return t;
        });

        loadFromFile();
    }

    /**
     * Adds or updates an alert rule in the repository, triggers auto-subscription if provider exists,
     * and asynchronously persists state.
     *
     * @param rule the alert rule to store
     */
    public void addRule(AlertRule rule) {
        Objects.requireNonNull(rule, "rule must not be null");
        rules.put(rule.id(), rule);
        ensureSubscribed(rule.symbol());
        persistAsync();
    }

    /**
     * Retrieves an alert rule by its ID.
     *
     * @param id alert rule ID
     * @return the alert rule, or null if not found
     */
    public AlertRule getRule(String id) {
        if (id == null) {
            return null;
        }
        return rules.get(id);
    }

    /**
     * Deletes an alert rule by ID.
     *
     * @param id alert rule ID
     * @return true if a rule was removed, false otherwise
     */
    public boolean deleteRule(String id) {
        if (id == null) {
            return false;
        }
        boolean removed = rules.remove(id) != null;
        if (removed) {
            persistAsync();
        }
        return removed;
    }

    /**
     * Updates the status of an existing alert rule.
     *
     * @param id     alert rule ID
     * @param status new status (ACTIVE, TRIGGERED, DISABLED)
     * @return updated AlertRule, or null if rule not found
     */
    public AlertRule updateStatus(String id, AlertStatus status) {
        if (id == null || status == null) {
            return null;
        }
        AlertRule existing = rules.get(id);
        if (existing == null) {
            return null;
        }
        AlertRule updated = existing.withStatus(status);
        rules.put(id, updated);
        persistAsync();
        return updated;
    }

    /**
     * Re-arms an alert rule with ACTIVE status and resets its reference price.
     *
     * @param id                alert rule ID
     * @param newReferencePrice current price to prevent instant false positive
     * @return re-armed AlertRule, or null if rule not found
     */
    public AlertRule rearmRule(String id, double newReferencePrice) {
        if (id == null) {
            return null;
        }
        AlertRule existing = rules.get(id);
        if (existing == null) {
            return null;
        }
        AlertRule rearmed = existing.rearm(newReferencePrice);
        rules.put(id, rearmed);
        persistAsync();
        return rearmed;
    }

    /**
     * Updates an alert rule when triggered (e.g. setting TRIGGERED status or advancing cooldown timestamp).
     *
     * @param updatedRule modified rule
     */
    public void updateRule(AlertRule updatedRule) {
        if (updatedRule == null) {
            return;
        }
        rules.put(updatedRule.id(), updatedRule);
        persistAsync();
    }

    /**
     * Returns a list of all currently ACTIVE alert rules.
     *
     * @return list of active rules
     */
    public List<AlertRule> getActiveRules() {
        List<AlertRule> active = new ArrayList<>();
        for (AlertRule rule : rules.values()) {
            if (rule.status() == AlertStatus.ACTIVE) {
                active.add(rule);
            }
        }
        return Collections.unmodifiableList(active);
    }

    /**
     * Returns a list of all alert rules sorted by creation time descending.
     *
     * @return list of all alert rules
     */
    public List<AlertRule> getAllRules() {
        List<AlertRule> all = new ArrayList<>(rules.values());
        all.sort(Comparator.comparingLong(AlertRule::createdAtEpochMs).reversed());
        return Collections.unmodifiableList(all);
    }

    /**
     * Resolves the Telegram configuration, falling back to environment variables
     * (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) if not explicitly configured or empty.
     *
     * @return resolved TelegramConfig
     */
    public TelegramConfig getTelegramConfig() {
        TelegramConfig current = telegramConfig.get();
        String botToken = (current != null && !current.botToken().isBlank())
            ? current.botToken()
            : getEnvOrDefault(ENV_TELEGRAM_BOT_TOKEN, "");

        String chatId = (current != null && !current.chatId().isBlank())
            ? current.chatId()
            : getEnvOrDefault(ENV_TELEGRAM_CHAT_ID, "");

        boolean enabled = (current != null)
            ? current.enabled()
            : (!botToken.isBlank() && !chatId.isBlank());

        return new TelegramConfig(botToken, chatId, enabled);
    }

    /**
     * Updates Telegram configuration and persists asynchronously.
     *
     * @param config new Telegram configuration
     */
    public void updateTelegramConfig(TelegramConfig config) {
        telegramConfig.set(config);
        persistAsync();
    }

    /**
     * Records a triggered AlertEvent into history, maintaining a maximum capacity of 50 items.
     *
     * @param event triggered alert event
     */
    public void recordEvent(AlertEvent event) {
        if (event == null) {
            return;
        }
        history.addFirst(event);
        while (history.size() > MAX_HISTORY_SIZE) {
            history.pollLast();
        }
        persistAsync();
    }

    /**
     * Returns the chronological history of triggered alerts (newest first, max 50 items).
     *
     * @return list of alert events
     */
    public List<AlertEvent> getHistory() {
        return Collections.unmodifiableList(new ArrayList<>(history));
    }

    /**
     * Asynchronously schedules persistence to the configured JSON file.
     */
    public void persistAsync() {
        if (!persistenceExecutor.isShutdown()) {
            persistenceExecutor.submit(this::saveSync);
        }
    }

    /**
     * Synchronously persists repository state to storagePath using atomic replacement.
     */
    public synchronized void saveSync() {
        try {
            Path parent = storagePath.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }

            String json = AlertJsonCodec.serialize(
                telegramConfig.get(),
                new ArrayList<>(rules.values()),
                new ArrayList<>(history)
            );

            Path dir = parent != null ? parent : Path.of(".");
            Path tmpPath = Files.createTempFile(dir, "alerts-", ".tmp");
            try {
                Files.writeString(
                    tmpPath,
                    json,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
                );

                try {
                    Files.move(tmpPath, storagePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (Exception e) {
                    // Windows atomic move fallback
                    Files.move(tmpPath, storagePath, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(tmpPath);
            }
        } catch (IOException e) {
            System.err.println("[AlertRepository] Failed to persist alerts to " + storagePath + ": " + e.getMessage());
        }
    }

    /**
     * Loads rules, history, and Telegram configuration from the storage file if it exists.
     */
    public synchronized void loadFromFile() {
        if (!Files.exists(storagePath)) {
            return;
        }

        try {
            String json = Files.readString(storagePath, StandardCharsets.UTF_8);
            AlertJsonCodec.Snapshot snapshot = AlertJsonCodec.deserialize(json);

            if (snapshot.telegramConfig() != null) {
                telegramConfig.set(snapshot.telegramConfig());
            }

            rules.clear();
            if (snapshot.rules() != null) {
                for (AlertRule r : snapshot.rules()) {
                    rules.put(r.id(), r);
                    ensureSubscribed(r.symbol());
                }
            }

            history.clear();
            if (snapshot.history() != null) {
                for (AlertEvent e : snapshot.history()) {
                    if (history.size() < MAX_HISTORY_SIZE) {
                        history.add(e);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[AlertRepository] Failed to load alerts from " + storagePath + ": " + e.getMessage());
        }
    }

    private void ensureSubscribed(String symbol) {
        if (provider != null && symbol != null && !symbol.isBlank()) {
            List<String> current = provider.getSubscribedSymbols();
            if (current == null || !current.contains(symbol)) {
                provider.subscribe(List.of(symbol));
            }
        }
    }

    private static String getEnvOrDefault(String key, String defaultVal) {
        try {
            String val = System.getenv(key);
            return (val != null && !val.isBlank()) ? val.trim() : defaultVal;
        } catch (Exception ignored) {
            return defaultVal;
        }
    }

    @Override
    public void close() {
        persistenceExecutor.shutdown();
        try {
            if (!persistenceExecutor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)) {
                persistenceExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            persistenceExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        saveSync();
    }
}
