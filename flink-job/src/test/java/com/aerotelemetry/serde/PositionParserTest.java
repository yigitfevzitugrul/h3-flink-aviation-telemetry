package com.aerotelemetry.serde;

import com.aerotelemetry.model.AircraftPosition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PositionParserTest {

    @Test
    void parsesProducerJson() {
        String json = "{\"icao24\":\"4B9E63\",\"callsign\":\"KKK7771\",\"lat\":38.03348,\"lon\":27.42514,"
                + "\"baro_altitude\":10668.0,\"velocity\":230.0,\"heading\":140.1,\"vertical_rate\":0.0,"
                + "\"squawk\":\"2000\",\"on_ground\":false,\"event_time\":1792768809061,\"source\":\"sim\","
                + "\"extra_field\":1}";

        AircraftPosition p = PositionParser.parse(json);

        assertEquals("4b9e63", p.icao24);
        assertEquals(38.03348, p.lat);
        assertEquals(10668.0, p.baroAltitude);
        assertEquals(0.0, p.verticalRate);
        assertEquals(1792768809061L, p.eventTime);
        assertFalse(p.onGround);
    }

    @Test
    void acceptsNullOptionalFields() {
        String json = "{\"icao24\":\"abc123\",\"lat\":40,\"lon\":30,\"baro_altitude\":null,"
                + "\"callsign\":null,\"event_time\":1}";
        AircraftPosition p = PositionParser.parse(json);
        assertNull(p.baroAltitude);
        assertNull(p.callsign);
    }

    @Test
    void rejectsMalformedJson() {
        assertThrows(PositionParser.InvalidRecordException.class, () -> PositionParser.parse("{not json"));
    }

    @Test
    void rejectsMissingIcao() {
        assertThrows(PositionParser.InvalidRecordException.class,
                () -> PositionParser.parse("{\"lat\":40,\"lon\":30,\"event_time\":1}"));
    }

    @Test
    void rejectsOutOfRangeCoordinates() {
        assertThrows(PositionParser.InvalidRecordException.class,
                () -> PositionParser.parse("{\"icao24\":\"a\",\"lat\":95,\"lon\":30,\"event_time\":1}"));
    }

    @Test
    void rejectsMissingEventTime() {
        assertThrows(PositionParser.InvalidRecordException.class,
                () -> PositionParser.parse("{\"icao24\":\"a\",\"lat\":40,\"lon\":30}"));
    }
}
