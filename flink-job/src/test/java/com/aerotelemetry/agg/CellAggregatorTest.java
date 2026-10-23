package com.aerotelemetry.agg;

import com.aerotelemetry.model.AircraftPosition;
import com.aerotelemetry.model.CellStats;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CellAggregatorTest {

    private final CellAggregator agg = new CellAggregator();

    private static AircraftPosition pos(String icao, Double alt, Double speed) {
        AircraftPosition p = new AircraftPosition();
        p.icao24 = icao;
        p.baroAltitude = alt;
        p.velocity = speed;
        return p;
    }

    @Test
    void countsDistinctAircraftAndAveragesAllSamples() {
        CellAggregator.Acc acc = agg.createAccumulator();
        acc = agg.add(pos("a", 10000.0, 200.0), acc);
        acc = agg.add(pos("a", 10200.0, 220.0), acc);   // aynı uçak, ikinci mesaj
        acc = agg.add(pos("b", 9800.0, 240.0), acc);

        CellStats s = agg.getResult(acc);
        assertEquals(2, s.aircraftCount);
        assertEquals(10000.0, s.avgAltitudeM, 1e-9);
        assertEquals(220.0, s.avgSpeedMs, 1e-9);
    }

    @Test
    void ignoresMissingValuesInAverages() {
        CellAggregator.Acc acc = agg.createAccumulator();
        acc = agg.add(pos("a", null, null), acc);
        acc = agg.add(pos("b", 5000.0, null), acc);

        CellStats s = agg.getResult(acc);
        assertEquals(2, s.aircraftCount);
        assertEquals(5000.0, s.avgAltitudeM, 1e-9);
        assertNull(s.avgSpeedMs);
    }

    @Test
    void mergeCombinesAccumulators() {
        CellAggregator.Acc a = agg.add(pos("a", 1000.0, 100.0), agg.createAccumulator());
        CellAggregator.Acc b = agg.add(pos("a", 3000.0, 300.0), agg.createAccumulator());
        b = agg.add(pos("c", 2000.0, 200.0), b);

        CellStats s = agg.getResult(agg.merge(a, b));
        assertEquals(2, s.aircraftCount);
        assertEquals(2000.0, s.avgAltitudeM, 1e-9);
        assertEquals(200.0, s.avgSpeedMs, 1e-9);
    }
}
