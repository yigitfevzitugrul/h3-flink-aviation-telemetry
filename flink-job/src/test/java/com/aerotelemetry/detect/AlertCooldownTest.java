package com.aerotelemetry.detect;

import com.aerotelemetry.model.Alert;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AlertCooldownTest {

    private static Alert alert(String pair, long t) {
        Alert a = new Alert();
        a.alertType = Alert.PROXIMITY;
        a.icao24 = pair;
        a.eventTime = t;
        return a;
    }

    @Test
    void suppressesRepeatsWithinCooldownPerKey() throws Exception {
        try (var harness = new KeyedOneInputStreamOperatorTestHarness<String, Alert, Alert>(
                new KeyedProcessOperator<>(new AlertCooldown(Duration.ofMinutes(5))), a -> a.icao24, Types.STRING)) {
            harness.open();
            harness.processElement(alert("x", 0), 0);
            harness.processElement(alert("x", 60_000), 60_000);    // bastırılır
            harness.processElement(alert("y", 60_000), 60_000);    // farklı çift
            harness.processElement(alert("x", 300_000), 300_000);  // süre doldu

            List<Alert> out = harness.extractOutputValues();
            assertEquals(List.of("x@0", "y@60000", "x@300000"),
                    out.stream().map(a -> a.icao24 + "@" + a.eventTime).toList());
        }
    }
}
