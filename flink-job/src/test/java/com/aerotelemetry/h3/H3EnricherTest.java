package com.aerotelemetry.h3;

import com.aerotelemetry.model.AircraftPosition;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import org.apache.flink.api.common.functions.OpenContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class H3EnricherTest {

    private static H3Core h3;
    private static H3Enricher enricher;

    @BeforeAll
    static void setUp() throws IOException {
        h3 = H3Core.newInstance();
        enricher = new H3Enricher();
        enricher.open((OpenContext) null);
    }

    private static AircraftPosition at(double lat, double lon) {
        AircraftPosition p = new AircraftPosition();
        p.icao24 = "abc123";
        p.lat = lat;
        p.lon = lon;
        return p;
    }

    @Test
    void assignsCellsAtExpectedResolutions() {
        AircraftPosition p = enricher.map(at(41.2753, 28.7519));   // İstanbul Havalimanı
        assertEquals(H3Enricher.REGION_RES, h3.getResolution(p.h3Region));
        assertEquals(H3Enricher.PROXIMITY_RES, h3.getResolution(p.h3Proximity));
    }

    @Test
    void proximityCellIsNestedInRegionCell() {
        AircraftPosition p = enricher.map(at(39.9334, 32.8597));   // Ankara
        assertEquals(p.h3Region, h3.cellToParentAddress(p.h3Proximity, H3Enricher.REGION_RES));
    }

    @Test
    void cellCenterIsNearOriginalPoint() {
        AircraftPosition p = enricher.map(at(38.4237, 27.1428));   // İzmir
        LatLng center = h3.cellToLatLng(p.h3Proximity);
        // res-8 hücre kenarı ~0.46 km: merkez 0.01 dereceden (~1 km) yakın olmalı
        assertEquals(38.4237, center.lat, 0.01);
        assertEquals(27.1428, center.lng, 0.01);
    }

    @Test
    void nearbyPointsAreAFewProximityCellsApart() throws Exception {
        AircraftPosition a = enricher.map(at(39.90, 32.85));
        AircraftPosition b = enricher.map(at(39.90, 32.88));   // ~2.5 km doğu
        long distance = h3.gridDistance(a.h3Proximity, b.h3Proximity);
        assertTrue(distance >= 2 && distance <= 5, "grid mesafesi: " + distance);
    }
}
