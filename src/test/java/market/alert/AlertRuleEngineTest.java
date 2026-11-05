package market.alert;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class AlertRuleEngineTest {

    @TempDir
    Path tempDir;

    private AlertRepository repository;
    private AlertRuleEngine engine;
    private List<AlertEvent> capturedEvents;

    @BeforeEach
    void setUp() {
        Path alertsFile = tempDir.resolve("test-alerts.json");
        repository = new AlertRepository(alertsFile);
        capturedEvents = new ArrayList<>();
        engine = new AlertRuleEngine(repository, null, (rule, event) -> capturedEvents.add(event));
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        if (repository != null) {
            repository.close();
        }
    }

    @Test
    @DisplayName("Should detect genuine ABOVE threshold crossing when price climbs over target")
    void shouldDetectAboveCrossing() {
        AlertRule rule = new AlertRule(
            UUID.randomUUID().toString(),
            "RELIANCE.NS",
            AlertCondition.ABOVE,
            3000.0,
            TriggerMode.ONE_SHOT,
            15,
            false,
            true,
            AlertStatus.ACTIVE,
            2950.0,
            0L,
            1000L
        );
        repository.addRule(rule);

        // Tick 1: Still below threshold
        engine.onTick("RELIANCE.NS", 2980.0, 2000L);
        assertThat(capturedEvents).isEmpty();
        assertThat(repository.getRule(rule.id()).status()).isEqualTo(AlertStatus.ACTIVE);

        // Tick 2: Crosses above threshold
        engine.onTick("RELIANCE.NS", 3005.0, 3000L);
        assertThat(capturedEvents).hasSize(1);
        AlertEvent event = capturedEvents.getFirst();
        assertThat(event.symbol()).isEqualTo("RELIANCE.NS");
        assertThat(event.condition()).isEqualTo(AlertCondition.ABOVE);
        assertThat(event.triggerPrice()).isEqualTo(3005.0);
        assertThat(event.targetPrice()).isEqualTo(3000.0);
        assertThat(repository.getRule(rule.id()).status()).isEqualTo(AlertStatus.TRIGGERED);
    }

    @Test
    @DisplayName("Should detect genuine BELOW threshold crossing when price drops below target")
    void shouldDetectBelowCrossing() {
        AlertRule rule = new AlertRule(
            UUID.randomUUID().toString(),
            "TCS.NS",
            AlertCondition.BELOW,
            4000.0,
            TriggerMode.ONE_SHOT,
            15,
            false,
            true,
            AlertStatus.ACTIVE,
            4050.0,
            0L,
            1000L
        );
        repository.addRule(rule);

        // Tick 1: Above threshold
        engine.onTick("TCS.NS", 4020.0, 2000L);
        assertThat(capturedEvents).isEmpty();

        // Tick 2: Crosses below threshold
        engine.onTick("TCS.NS", 3995.0, 3000L);
        assertThat(capturedEvents).hasSize(1);
        AlertEvent event = capturedEvents.getFirst();
        assertThat(event.symbol()).isEqualTo("TCS.NS");
        assertThat(event.condition()).isEqualTo(AlertCondition.BELOW);
        assertThat(event.triggerPrice()).isEqualTo(3995.0);
        assertThat(repository.getRule(rule.id()).status()).isEqualTo(AlertStatus.TRIGGERED);
    }

    @Test
    @DisplayName("Should detect threshold crossing on first tick if referencePrice was on opposite side")
    void shouldDetectCrossingOnFirstTickUsingReferencePrice() {
        AlertRule rule = new AlertRule(
            UUID.randomUUID().toString(),
            "AAPL",
            AlertCondition.ABOVE,
            200.0,
            TriggerMode.ONE_SHOT,
            15,
            false,
            true,
            AlertStatus.ACTIVE,
            195.0, // referencePrice < targetPrice
            0L,
            1000L
        );
        repository.addRule(rule);

        // First tick arrives already above target
        engine.onTick("AAPL", 202.0, 2000L);
        assertThat(capturedEvents).hasSize(1);
        assertThat(repository.getRule(rule.id()).status()).isEqualTo(AlertStatus.TRIGGERED);
    }

    @Test
    @DisplayName("Should NOT trigger on first tick if referencePrice was already on breached side (not a true crossing)")
    void shouldNotTriggerIfReferencePriceAlreadyBreached() {
        AlertRule rule = new AlertRule(
            UUID.randomUUID().toString(),
            "AAPL",
            AlertCondition.ABOVE,
            200.0,
            TriggerMode.ONE_SHOT,
            15,
            false,
            true,
            AlertStatus.ACTIVE,
            205.0, // referencePrice already >= targetPrice
            0L,
            1000L
        );
        repository.addRule(rule);

        // First tick at 206
        engine.onTick("AAPL", 206.0, 2000L);
        assertThat(capturedEvents).isEmpty();

        // Price dips below target
        engine.onTick("AAPL", 198.0, 3000L);
        assertThat(capturedEvents).isEmpty();

        // Price crosses back above target
        engine.onTick("AAPL", 201.0, 4000L);
        assertThat(capturedEvents).hasSize(1);
        assertThat(repository.getRule(rule.id()).status()).isEqualTo(AlertStatus.TRIGGERED);
    }

    @Test
    @DisplayName("ONE_SHOT rule must only fire once and transition to TRIGGERED")
    void shouldOnlyFireOnceForOneShotRule() {
        AlertRule rule = new AlertRule(
            UUID.randomUUID().toString(),
            "INFY.NS",
            AlertCondition.ABOVE,
            1500.0,
            TriggerMode.ONE_SHOT,
            15,
            false,
            true,
            AlertStatus.ACTIVE,
            1480.0,
            0L,
            1000L
        );
        repository.addRule(rule);

        // Tick 1: Crosses
        engine.onTick("INFY.NS", 1510.0, 2000L);
        assertThat(capturedEvents).hasSize(1);
        assertThat(repository.getRule(rule.id()).status()).isEqualTo(AlertStatus.TRIGGERED);

        // Tick 2: Stays above threshold - should not fire again
        engine.onTick("INFY.NS", 1520.0, 3000L);
        assertThat(capturedEvents).hasSize(1);

        // Tick 3: Dips and rises again - should still not fire
        engine.onTick("INFY.NS", 1490.0, 4000L);
        engine.onTick("INFY.NS", 1515.0, 5000L);
        assertThat(capturedEvents).hasSize(1);
    }

    @Test
    @DisplayName("RECURRING rule should clamp triggers during cooldown window and fire again after cooldown")
    void shouldClampTriggersDuringCooldownWindow() {
        int cooldownMin = 5; // 300,000 ms
        AlertRule rule = new AlertRule(
            UUID.randomUUID().toString(),
            "HDFCBANK.NS",
            AlertCondition.ABOVE,
            1600.0,
            TriggerMode.RECURRING,
            cooldownMin,
            false,
            true,
            AlertStatus.ACTIVE,
            1580.0,
            0L,
            1000L
        );
        repository.addRule(rule);

        long t0 = 100_000L;
        // Initial crossing: fires
        engine.onTick("HDFCBANK.NS", 1605.0, t0);
        assertThat(capturedEvents).hasSize(1);
        AlertRule updatedRule = repository.getRule(rule.id());
        assertThat(updatedRule.status()).isEqualTo(AlertStatus.ACTIVE);
        assertThat(updatedRule.lastTriggeredAtEpochMs()).isEqualTo(t0);

        // 2 minutes later (120,000 ms): inside cooldown -> clamped
        engine.onTick("HDFCBANK.NS", 1610.0, t0 + 120_000L);
        assertThat(capturedEvents).hasSize(1);

        // 4 minutes 59 seconds later (299,000 ms): inside cooldown -> clamped
        engine.onTick("HDFCBANK.NS", 1608.0, t0 + 299_000L);
        assertThat(capturedEvents).hasSize(1);

        // 5 minutes exactly (300,000 ms): cooldown elapsed and still breached -> fires!
        long t1 = t0 + 300_000L;
        engine.onTick("HDFCBANK.NS", 1612.0, t1);
        assertThat(capturedEvents).hasSize(2);
        assertThat(repository.getRule(rule.id()).lastTriggeredAtEpochMs()).isEqualTo(t1);

        // 10 seconds later: inside new cooldown -> clamped
        engine.onTick("HDFCBANK.NS", 1615.0, t1 + 10_000L);
        assertThat(capturedEvents).hasSize(2);

        // 5 minutes later again (600,000 ms after t0): fires 3rd time
        long t2 = t1 + 300_000L;
        engine.onTick("HDFCBANK.NS", 1620.0, t2);
        assertThat(capturedEvents).hasSize(3);
        assertThat(repository.getRule(rule.id()).lastTriggeredAtEpochMs()).isEqualTo(t2);
    }

    @Test
    @DisplayName("Should ignore ticks for DISABLED rules")
    void shouldIgnoreDisabledRules() {
        AlertRule rule = new AlertRule(
            UUID.randomUUID().toString(),
            "MSFT",
            AlertCondition.ABOVE,
            400.0,
            TriggerMode.ONE_SHOT,
            15,
            false,
            true,
            AlertStatus.DISABLED,
            390.0,
            0L,
            1000L
        );
        repository.addRule(rule);

        engine.onTick("MSFT", 405.0, 2000L);
        assertThat(capturedEvents).isEmpty();
    }

    @Test
    @DisplayName("Should handle case insensitive symbol matching")
    void shouldHandleCaseInsensitiveMatching() {
        AlertRule rule = new AlertRule(
            UUID.randomUUID().toString(),
            "reliance.ns",
            AlertCondition.ABOVE,
            3000.0,
            TriggerMode.ONE_SHOT,
            15,
            false,
            true,
            AlertStatus.ACTIVE,
            2950.0,
            0L,
            1000L
        );
        repository.addRule(rule);

        engine.onTick("RELIANCE.NS", 3010.0, 2000L);
        assertThat(capturedEvents).hasSize(1);
    }

    @Test
    @DisplayName("Should track last known prices for symbols")
    void shouldTrackLastKnownPrices() {
        engine.onTick("AAPL", 150.25, 1000L);
        engine.onTick("GOOG", 2800.50, 1000L);

        assertThat(engine.getLastKnownPrice("AAPL")).isEqualTo(150.25);
        assertThat(engine.getLastKnownPrice("aapl")).isEqualTo(150.25);
        assertThat(engine.getLastKnownPrice("GOOG")).isEqualTo(2800.50);
        assertThat(engine.getLastKnownPrice("UNKNOWN")).isNull();
    }
}
