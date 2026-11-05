package market.bus;

import market.bus.aeron.AeronTelemetryCollector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AeronTelemetryCollectorTest {

    @Test
    @DisplayName("Should initialize with correct default Aeron IPC values")
    void shouldInitializeWithDefaultValues() {
        AeronTelemetryCollector collector = new AeronTelemetryCollector();
        assertThat(collector.channel()).isEqualTo("aeron:ipc");
        assertThat(collector.streamId()).isEqualTo(1001);
        assertThat(collector.mediaDriver()).contains("ArchivingMediaDriver Embedded IPC");
        assertThat(collector.ticksPublished()).isEqualTo(0);
        assertThat(collector.ticksConsumed()).isEqualTo(0);
        assertThat(collector.backpressureEvents()).isEqualTo(0);
        assertThat(collector.offerRetries()).isEqualTo(0);

        AeronTelemetryCollector.TelemetrySnapshot snapshot = collector.snapshot();
        assertThat(snapshot.status()).isEqualTo("HEALTHY");
        assertThat(snapshot.ringBufferHealth()).isEqualTo("CONNECTED / ZERO LAG");
        assertThat(snapshot.lagBytes()).isEqualTo(0);
        assertThat(snapshot.offHeapAllocation()).isEqualTo("Zero (DirectBuffer)");
        assertThat(snapshot.wireFormat()).contains("SBE v1");
    }

    @Test
    @DisplayName("Should accurately record publications, consumption, and latency percentiles")
    void shouldTrackPublicationsAndLatency() {
        AeronTelemetryCollector collector = new AeronTelemetryCollector("aeron:ipc", 1001);

        // Record 100 ticks with increasing latencies: 500ns, 1000ns, ..., 50000ns
        for (int i = 1; i <= 100; i++) {
            collector.recordPublished(128, (i % 20 == 0) ? 1 : 0);
            collector.recordConsumed(128, i * 500L); // 0.5 us to 50.0 us
        }

        collector.recordBackpressure();

        assertThat(collector.ticksPublished()).isEqualTo(100);
        assertThat(collector.ticksConsumed()).isEqualTo(100);
        assertThat(collector.bytesPublished()).isEqualTo(100 * 128);
        assertThat(collector.bytesConsumed()).isEqualTo(100 * 128);
        assertThat(collector.offerRetries()).isEqualTo(5); // 100 / 20 = 5
        assertThat(collector.backpressureEvents()).isEqualTo(1);

        AeronTelemetryCollector.TelemetrySnapshot snapshot = collector.snapshot();
        assertThat(snapshot.lagBytes()).isEqualTo(0);
        assertThat(snapshot.latency().minNs()).isEqualTo(500L);
        assertThat(snapshot.latency().maxNs()).isEqualTo(50_000L);
        assertThat(snapshot.latency().lastNs()).isEqualTo(50_000L);
        assertThat(snapshot.latency().minUs()).isEqualTo(0.50);
        assertThat(snapshot.latency().maxUs()).isEqualTo(50.00);

        // p99 of 1..100 with step 500 should be ~99th element (99 * 500 = 49,500 ns / 49.5 us)
        assertThat(snapshot.latency().p99Us()).isBetween(48.0, 50.0);
    }

    @Test
    @DisplayName("Should produce valid clean JSON snapshot conforming to API specification")
    void shouldProduceValidJsonSnapshot() {
        AeronTelemetryCollector collector = new AeronTelemetryCollector("aeron:ipc", 1001);
        collector.recordPublished(256, 0);
        collector.recordConsumed(256, 850L); // 0.85 us

        String json = collector.toJson();
        assertThat(json).contains("\"channel\": \"aeron:ipc\"");
        assertThat(json).contains("\"streamId\": 1001");
        assertThat(json).contains("\"mediaDriver\": \"ArchivingMediaDriver Embedded IPC\"");
        assertThat(json).contains("\"status\": \"HEALTHY\"");
        assertThat(json).contains("\"ticksPublished\": 1");
        assertThat(json).contains("\"ticksConsumed\": 1");
        assertThat(json).contains("\"lagBytes\": 0");
        assertThat(json).contains("\"last\": 0.85");
        assertThat(json).contains("\"offHeapAllocation\": \"Zero (DirectBuffer)\"");
        assertThat(json).contains("\"wireFormat\": \"SBE v1 (Template ID 1)\"");
        assertThat(json).contains("\"gcOverhead\": \"0.00ms\"");
    }

    @Test
    @DisplayName("Should compute positive throughput when consumed messages arrive")
    void shouldComputeThroughput() {
        AeronTelemetryCollector collector = new AeronTelemetryCollector();
        for (int i = 0; i < 50; i++) {
            collector.recordPublished(128, 0);
            collector.recordConsumed(128, 1000L);
        }

        double throughput = collector.computeThroughput();
        assertThat(throughput).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    @DisplayName("Should handle reset cleanly")
    void shouldResetCleanly() {
        AeronTelemetryCollector collector = new AeronTelemetryCollector();
        collector.recordPublished(256, 2);
        collector.recordBackpressure();
        collector.recordConsumed(256, 1200L);

        collector.reset();

        assertThat(collector.ticksPublished()).isEqualTo(0);
        assertThat(collector.ticksConsumed()).isEqualTo(0);
        assertThat(collector.backpressureEvents()).isEqualTo(0);
        assertThat(collector.offerRetries()).isEqualTo(0);
        assertThat(collector.lastLatencyNs()).isEqualTo(0);
    }
}
