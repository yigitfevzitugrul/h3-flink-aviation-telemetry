package com.aerotelemetry.detect;

import com.aerotelemetry.model.AircraftPosition;
import com.aerotelemetry.model.Alert;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AnomalyDetectorTest {

    private KeyedOneInputStreamOperatorTestHarness<String, AircraftPosition, Alert> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new AnomalyDetector()), p -> p.icao24, Types.STRING);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    /** t saniyesinde, doğuya 230 m/s ile uçan uçak (~0.0027° boylam / sn). */
    private static AircraftPosition pos(int t, double alt, double verticalRate, String squawk) {
        AircraftPosition p = new AircraftPosition();
        p.icao24 = "4b1234";
        p.callsign = "TEST1";
        p.lat = 39.0;
        p.lon = 30.0 + t * 0.00266;
        p.baroAltitude = alt;
        p.velocity = 230.0;
        p.verticalRate = verticalRate;
        p.squawk = squawk;
        p.eventTime = 1_000_000L + t * 1000L;
        return p;
    }

    private void send(AircraftPosition p) throws Exception {
        harness.processElement(p, p.eventTime);
    }

    private List<Alert> alerts() {
        return harness.extractOutputValues();
    }

    @Test
    void normalFlightProducesNoAlerts() throws Exception {
        for (int t = 0; t <= 60; t += 5) {
            send(pos(t, 10_000, -7.0, "2000"));
        }
        assertTrue(alerts().isEmpty());
    }

    @Test
    void emergencySquawkIsReportedOnceOnTransition() throws Exception {
        send(pos(0, 10_000, 0, "2000"));
        send(pos(5, 10_000, 0, "7700"));
        send(pos(10, 10_000, 0, "7700"));
        send(pos(15, 10_000, 0, "7700"));

        List<Alert> out = alerts();
        assertEquals(1, out.size());
        assertEquals(Alert.EMERGENCY_SQUAWK, out.get(0).alertType);
        assertTrue(out.get(0).details.contains("7700"));
    }

    @Test
    void rapidDescentIsReportedOncePerEpisode() throws Exception {
        send(pos(0, 10_000, 0, "2000"));
        send(pos(5, 9_775, -45, "2000"));    // bölüm 1 başlar
        send(pos(10, 9_550, -45, "2000"));
        send(pos(15, 9_550, 0, "2000"));     // biter
        send(pos(20, 9_325, -45, "2000"));   // bölüm 2

        List<Alert> out = alerts();
        assertEquals(2, out.size());
        assertTrue(out.stream().allMatch(a -> a.alertType.equals(Alert.RAPID_DESCENT)));
    }

    @Test
    void steepDescentNearGroundIsIgnored() throws Exception {
        send(pos(0, 900, 0, "2000"));
        send(pos(5, 700, -40, "2000"));
        assertTrue(alerts().isEmpty());
    }

    @Test
    void descentIsDerivedFromAltitudeWhenVerticalRateMissing() throws Exception {
        AircraftPosition a = pos(0, 10_000, 0, "2000");
        AircraftPosition b = pos(5, 9_750, 0, "2000");   // -50 m/s
        b.verticalRate = null;
        send(a);
        send(b);
        assertEquals(Alert.RAPID_DESCENT, alerts().get(0).alertType);
    }

    @Test
    void gpsGlitchIsReportedAndDiscarded() throws Exception {
        send(pos(0, 10_000, 0, "2000"));
        AircraftPosition glitch = pos(5, 10_000, 0, "2000");
        glitch.lat += 2.0;   // ~220 km sıçrama
        send(glitch);
        send(pos(10, 10_000, 0, "2000"));   // gerçek rotaya dönüş: yeni sıçrama sayılmamalı

        List<Alert> out = alerts();
        assertEquals(1, out.size());
        assertEquals(Alert.SPEED_JUMP, out.get(0).alertType);
    }

    @Test
    void outOfOrderMessagesAreIgnored() throws Exception {
        send(pos(10, 10_000, 0, "2000"));
        send(pos(5, 10_000, 0, "7700"));   // eski mesaj
        assertTrue(alerts().isEmpty());
    }
}
