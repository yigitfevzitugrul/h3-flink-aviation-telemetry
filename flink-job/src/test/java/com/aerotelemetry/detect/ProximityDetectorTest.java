package com.aerotelemetry.detect;

import com.aerotelemetry.model.AircraftPosition;
import com.aerotelemetry.model.Alert;
import com.aerotelemetry.model.CellProbe;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProximityDetectorTest {

    private KeyedOneInputStreamOperatorTestHarness<String, CellProbe, Alert> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new ProximityDetector()), p -> p.targetCell, Types.STRING);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    private static AircraftPosition pos(String icao, String homeCell, double lat, double lon, double alt, long t) {
        AircraftPosition p = new AircraftPosition();
        p.icao24 = icao;
        p.callsign = icao.toUpperCase();
        p.h3Proximity = homeCell;
        p.lat = lat;
        p.lon = lon;
        p.baroAltitude = alt;
        p.eventTime = t;
        return p;
    }

    /** ProximityFanOut'un yaptığı gibi: kendi hücresine home, verilen komşulara sorgu probe'u. */
    private void fanOut(AircraftPosition p, String... neighbours) throws Exception {
        harness.processElement(new CellProbe(p.h3Proximity, true, p), p.eventTime);
        for (String n : neighbours) {
            harness.processElement(new CellProbe(n, false, p), p.eventTime);
        }
    }

    private List<Alert> alerts() {
        return harness.extractOutputValues();
    }

    @Test
    void closePairInSameCellIsReported() throws Exception {
        fanOut(pos("aaa111", "c1", 39.0000, 30.0, 10_000, 1_000));
        fanOut(pos("bbb222", "c1", 39.0050, 30.0, 10_100, 2_000));   // ~555 m kuzey, 100 m üst

        List<Alert> out = alerts();
        assertEquals(1, out.size());
        Alert a = out.get(0);
        assertEquals(Alert.PROXIMITY, a.alertType);
        assertEquals("aaa111", a.icao24);
        assertEquals("bbb222", a.otherIcao24);
    }

    @Test
    void pairAcrossCellBoundaryIsReportedExactlyOnce() throws Exception {
        AircraftPosition a = pos("aaa111", "c1", 39.0000, 30.0, 10_000, 1_000);
        AircraftPosition b = pos("bbb222", "c2", 39.0050, 30.0, 10_000, 1_000);
        fanOut(a, "c2");
        fanOut(b, "c1");
        // Bir sonraki turda tekrar karşılaşsalar da yalnızca küçük hücrede (c1) raporlanır
        fanOut(pos("aaa111", "c1", 39.0001, 30.0, 10_000, 6_000), "c2");
        fanOut(pos("bbb222", "c2", 39.0051, 30.0, 10_000, 6_000), "c1");

        List<Alert> out = alerts();
        assertEquals(2, out.size(), "her turda tek uyarı (tekrarları AlertCooldown bastırır)");
        assertTrue(out.stream().allMatch(x -> x.icao24.equals("aaa111") && x.otherIcao24.equals("bbb222")));
    }

    @Test
    void verticallySeparatedAircraftAreIgnored() throws Exception {
        fanOut(pos("aaa111", "c1", 39.0, 30.0, 10_000, 1_000));
        fanOut(pos("bbb222", "c1", 39.0, 30.0, 10_610, 1_000));   // 2000 ft fark
        assertTrue(alerts().isEmpty());
    }

    @Test
    void horizontallySeparatedAircraftAreIgnored() throws Exception {
        fanOut(pos("aaa111", "c1", 39.00, 30.0, 10_000, 1_000));
        fanOut(pos("bbb222", "c1", 39.02, 30.0, 10_000, 1_000));   // ~2.2 km
        assertTrue(alerts().isEmpty());
    }

    @Test
    void staleResidentIsNotComparedWithFreshPosition() throws Exception {
        fanOut(pos("aaa111", "c1", 39.0, 30.0, 10_000, 1_000));
        fanOut(pos("bbb222", "c1", 39.0, 30.0, 10_000, 1_000 + 20_000));   // 20 sn sonra
        assertTrue(alerts().isEmpty());
    }
}
