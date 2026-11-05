package market.alert;

import market.provider.MarketDataProvider;
import market.provider.RawMarketQuote;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

class AlertRepositoryTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Should add, retrieve, update, and delete alert rules")
    void shouldPerformCrudOperations() {
        Path file = tempDir.resolve("alerts-crud.json");
        try (AlertRepository repo = new AlertRepository(file)) {
            AlertRule rule1 = new AlertRule(
                "rule-1",
                "RELIANCE.NS",
                AlertCondition.ABOVE,
                3100.0,
                TriggerMode.ONE_SHOT,
                15,
                true,
                true,
                AlertStatus.ACTIVE,
                3050.0,
                0L,
                1000L
            );
            AlertRule rule2 = new AlertRule(
                "rule-2",
                "TCS.NS",
                AlertCondition.BELOW,
                3900.0,
                TriggerMode.RECURRING,
                30,
                false,
                true,
                AlertStatus.ACTIVE,
                3950.0,
                0L,
                2000L
            );

            repo.addRule(rule1);
            repo.addRule(rule2);

            assertThat(repo.getRule("rule-1")).isEqualTo(rule1);
            assertThat(repo.getRule("rule-2")).isEqualTo(rule2);
            assertThat(repo.getAllRules()).containsExactly(rule2, rule1); // sorted newest createdAt first
            assertThat(repo.getActiveRules()).hasSize(2);

            // Update status
            repo.updateStatus("rule-1", AlertStatus.TRIGGERED);
            assertThat(repo.getRule("rule-1").status()).isEqualTo(AlertStatus.TRIGGERED);
            assertThat(repo.getActiveRules()).containsExactly(rule2);

            // Delete rule
            boolean deleted = repo.deleteRule("rule-1");
            assertThat(deleted).isTrue();
            assertThat(repo.getRule("rule-1")).isNull();
            assertThat(repo.getAllRules()).containsExactly(rule2);
        }
    }

    @Test
    @DisplayName("Should maintain max 50 history events in newest-first order")
    void shouldClampHistoryToMax50Events() {
        Path file = tempDir.resolve("alerts-history.json");
        try (AlertRepository repo = new AlertRepository(file)) {
            for (int i = 1; i <= 60; i++) {
                AlertEvent event = new AlertEvent(
                    "rule-" + i,
                    "SYM" + i,
                    AlertCondition.ABOVE,
                    100.0 + i,
                    101.0 + i,
                    1000L + i,
                    "Triggered " + i
                );
                repo.recordEvent(event);
            }

            List<AlertEvent> history = repo.getHistory();
            assertThat(history).hasSize(50);
            // Newest event should be SYM60
            assertThat(history.getFirst().symbol()).isEqualTo("SYM60");
            // Oldest retained event should be SYM11 (1-10 were evicted)
            assertThat(history.getLast().symbol()).isEqualTo("SYM11");
        }
    }

    @Test
    @DisplayName("Should persist rules, history, and Telegram config and restore cleanly across instances")
    void shouldPersistAndRestoreCleanly() {
        Path file = tempDir.resolve("alerts-persist.json");

        AlertRule rule = new AlertRule(
            UUID.randomUUID().toString(),
            "INFY.NS",
            AlertCondition.ABOVE,
            1650.0,
            TriggerMode.RECURRING,
            15,
            true,
            true,
            AlertStatus.ACTIVE,
            1620.0,
            1000L,
            500L
        );
        AlertEvent event = new AlertEvent(
            rule.id(),
            "INFY.NS",
            AlertCondition.ABOVE,
            1650.0,
            1655.0,
            1000L,
            "INFY.NS breached"
        );
        TelegramConfig config = new TelegramConfig("12345:TOKEN", "987654321", true);

        // Instance 1: write and save
        try (AlertRepository repo1 = new AlertRepository(file)) {
            repo1.addRule(rule);
            repo1.recordEvent(event);
            repo1.updateTelegramConfig(config);
            repo1.saveSync();
        }

        assertThat(Files.exists(file)).isTrue();

        // Instance 2: reload from disk
        try (AlertRepository repo2 = new AlertRepository(file)) {
            AlertRule loadedRule = repo2.getRule(rule.id());
            assertThat(loadedRule).isNotNull();
            assertThat(loadedRule.symbol()).isEqualTo("INFY.NS");
            assertThat(loadedRule.targetPrice()).isEqualTo(1650.0);
            assertThat(loadedRule.triggerMode()).isEqualTo(TriggerMode.RECURRING);
            assertThat(loadedRule.cooldownMinutes()).isEqualTo(15);
            assertThat(loadedRule.lastTriggeredAtEpochMs()).isEqualTo(1000L);

            List<AlertEvent> loadedHistory = repo2.getHistory();
            assertThat(loadedHistory).hasSize(1);
            assertThat(loadedHistory.getFirst().symbol()).isEqualTo("INFY.NS");
            assertThat(loadedHistory.getFirst().triggerPrice()).isEqualTo(1655.0);

            TelegramConfig loadedConfig = repo2.getTelegramConfig();
            assertThat(loadedConfig.botToken()).isEqualTo("12345:TOKEN");
            assertThat(loadedConfig.chatId()).isEqualTo("987654321");
            assertThat(loadedConfig.enabled()).isTrue();
            assertThat(loadedConfig.masked().botToken()).isEqualTo("****OKEN");
        }
    }

    @Test
    @DisplayName("Should auto-subscribe missing symbols when rules are added")
    void shouldAutoSubscribeSymbolsWithProvider() {
        Path file = tempDir.resolve("alerts-subscribe.json");
        List<String> subscribed = new ArrayList<>();

        MarketDataProvider mockProvider = new MarketDataProvider() {
            @Override
            public void start() {}

            @Override
            public void stop() {}

            @Override
            public void subscribe(List<String> symbols) {
                subscribed.addAll(symbols);
            }

            @Override
            public List<String> getSubscribedSymbols() {
                return Collections.unmodifiableList(subscribed);
            }

            @Override
            public void registerTickListener(Consumer<RawMarketQuote> listener) {}
        };

        try (AlertRepository repo = new AlertRepository(file, mockProvider)) {
            AlertRule rule = new AlertRule(
                "rule-sub",
                "SBIN.NS",
                AlertCondition.ABOVE,
                800.0,
                TriggerMode.ONE_SHOT,
                15,
                true,
                true,
                AlertStatus.ACTIVE,
                780.0,
                0L,
                1000L
            );
            repo.addRule(rule);

            assertThat(subscribed).contains("SBIN.NS");
        }
    }
}
