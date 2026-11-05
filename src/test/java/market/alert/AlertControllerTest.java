package market.alert;

import market.provider.MarketDataProvider;
import market.provider.yahoo.YahooFinanceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AlertControllerTest {

    @TempDir
    Path tempDir;

    private AlertRepository repository;
    private AlertRuleEngine ruleEngine;
    private AlertController controller;
    private YahooFinanceProvider provider;

    @BeforeEach
    void setUp() {
        provider = new YahooFinanceProvider();
        provider.subscribe(List.of("RELIANCE.NS"));
        repository = new AlertRepository(tempDir.resolve("alerts.json"), provider);
        ruleEngine = new AlertRuleEngine(repository);
        controller = new AlertController(repository, ruleEngine, null, provider);
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        if (repository != null) {
            repository.close();
        }
    }

    @Test
    @DisplayName("Should return empty rules and history for newly initialized controller")
    void shouldReturnEmptyAlerts() {
        String json = controller.handleGetAlertsJson();
        assertThat(json).contains("\"rules\":[]");
        assertThat(json).contains("\"history\":[]");
        assertThat(json).contains("\"telegram\":");
    }

    @Test
    @DisplayName("Should create alert rule and auto-subscribe symbol")
    void shouldCreateAlertRule() {
        String payload = """
            {
                "symbol": "INFY.NS",
                "condition": "ABOVE",
                "targetPrice": 1950.50,
                "triggerMode": "ONE_SHOT",
                "cooldownMinutes": 15,
                "notifyTelegram": true,
                "notifyUi": true
            }
            """;

        String resp = controller.handleCreateAlertJson(payload);
        assertThat(resp).contains("\"success\":true");
        assertThat(resp).contains("\"symbol\":\"INFY.NS\"");
        assertThat(resp).contains("\"condition\":\"ABOVE\"");
        assertThat(resp).contains("\"targetPrice\":1950.5");
        assertThat(resp).contains("\"status\":\"ACTIVE\"");

        // Verify rule exists in repository
        List<AlertRule> rules = repository.getAllRules();
        assertThat(rules).hasSize(1);
        assertThat(rules.get(0).symbol()).isEqualTo("INFY.NS");

        // Verify symbol was auto-subscribed in provider
        assertThat(provider.getSubscribedSymbols()).contains("INFY.NS");
    }

    @Test
    @DisplayName("Should validate required fields when creating alert rule")
    void shouldValidateCreationPayload() {
        assertThatThrownBy(() -> controller.handleCreateAlertJson(null))
            .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> controller.handleCreateAlertJson("{}"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("symbol");

        assertThatThrownBy(() -> controller.handleCreateAlertJson("{\"symbol\":\"RELIANCE.NS\",\"targetPrice\":-5.0}"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Target price must be a positive number");

        assertThatThrownBy(() -> controller.handleCreateAlertJson("{\"symbol\":\"INVALID SYM!\",\"targetPrice\":100.0}"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid symbol format");
    }

    @Test
    @DisplayName("Should toggle alert rule status between ACTIVE, DISABLED, and re-arm")
    void shouldToggleAlertRule() {
        String createJson = """
            {
                "symbol": "RELIANCE.NS",
                "condition": "BELOW",
                "targetPrice": 2400.0,
                "triggerMode": "ONE_SHOT"
            }
            """;
        controller.handleCreateAlertJson(createJson);
        AlertRule created = repository.getAllRules().get(0);

        // Pause rule (active: false)
        String pauseResp = controller.handleToggleAlertJson("{\"id\":\"" + created.id() + "\",\"active\":false}");
        assertThat(pauseResp).contains("\"success\":true");
        assertThat(pauseResp).contains("\"status\":\"DISABLED\"");

        // Resume rule (active: true)
        String resumeResp = controller.handleToggleAlertJson("{\"id\":\"" + created.id() + "\",\"active\":true}");
        assertThat(resumeResp).contains("\"success\":true");
        assertThat(resumeResp).contains("\"status\":\"ACTIVE\"");
    }

    @Test
    @DisplayName("Should delete alert rule")
    void shouldDeleteAlertRule() {
        String createJson = """
            {
                "symbol": "TCS.NS",
                "condition": "ABOVE",
                "targetPrice": 4200.0
            }
            """;
        controller.handleCreateAlertJson(createJson);
        AlertRule created = repository.getAllRules().get(0);

        String delResp = controller.handleDeleteAlertJson("{\"id\":\"" + created.id() + "\"}");
        assertThat(delResp).isEqualTo("{\"success\":true}");
        assertThat(repository.getAllRules()).isEmpty();
    }

    @Test
    @DisplayName("Should save and query Telegram configuration with masked token")
    void shouldManageTelegramConfig() {
        String configPayload = """
            {
                "botToken": "123456789:AAEFGHijklmnopqrstuvwxyz",
                "chatId": "-100987654321",
                "enabled": true
            }
            """;
        String saveResp = controller.handleSaveTelegramConfigJson(configPayload);
        assertThat(saveResp).contains("\"success\":true");
        assertThat(saveResp).contains("\"configured\":true");
        assertThat(saveResp).contains("\"maskedToken\":\"****wxyz\"");
        assertThat(saveResp).contains("\"chatId\":\"-100987654321\"");
        assertThat(saveResp).contains("\"enabled\":true");

        String getResp = controller.handleGetTelegramConfigJson();
        assertThat(getResp).contains("\"maskedToken\":\"****wxyz\"");
    }

    @Test
    @DisplayName("Should reject Telegram test ping if not configured")
    void shouldRejectTelegramTestPingIfNotConfigured() {
        String testResp = controller.handleTestTelegramJson("{}");
        assertThat(testResp).contains("\"success\":false");
        assertThat(testResp).contains("Telegram Bot Token and Chat ID must be configured");
    }
}
