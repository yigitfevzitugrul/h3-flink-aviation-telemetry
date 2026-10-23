package com.aerotelemetry.serde;

import com.aerotelemetry.model.AircraftPosition;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

/** Producer'ın snake_case JSON mesajlarını {@link AircraftPosition}'a çevirir ve doğrular. */
public final class PositionParser {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private PositionParser() {}

    /**
     * @throws InvalidRecordException JSON bozuksa veya zorunlu alanlar geçersizse
     */
    public static AircraftPosition parse(String json) {
        AircraftPosition p;
        try {
            p = MAPPER.readValue(json, AircraftPosition.class);
        } catch (JsonProcessingException e) {
            throw new InvalidRecordException("JSON ayrıştırılamadı: " + e.getOriginalMessage());
        }
        if (p == null || p.icao24 == null || p.icao24.isBlank()) {
            throw new InvalidRecordException("icao24 eksik");
        }
        if (p.lat < -90 || p.lat > 90 || p.lon < -180 || p.lon > 180) {
            throw new InvalidRecordException("koordinat aralık dışı");
        }
        if (p.eventTime <= 0) {
            throw new InvalidRecordException("event_time eksik");
        }
        p.icao24 = p.icao24.toLowerCase();
        return p;
    }

    public static class InvalidRecordException extends RuntimeException {
        public InvalidRecordException(String message) {
            super(message);
        }
    }
}
