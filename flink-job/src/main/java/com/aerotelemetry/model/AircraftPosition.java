package com.aerotelemetry.model;

/**
 * Kafka'dan gelen tek bir uçak konum mesajı.
 *
 * <p>Flink POJO kurallarına uyar (public alanlar + boş kurucu), böylece Kryo yerine
 * Flink'in hızlı POJO serializer'ı kullanılır. Birimler SI: metre, m/s, epoch ms.
 */
public class AircraftPosition {

    public String icao24;
    public String callsign;
    public double lat;
    public double lon;
    public Double baroAltitude;
    public Double velocity;
    public Double heading;
    public Double verticalRate;
    public String squawk;
    public boolean onGround;
    public long eventTime;
    public String source;

    public AircraftPosition() {}

    @Override
    public String toString() {
        return String.format("%s[%s] (%.4f, %.4f) alt=%s v=%s sq=%s t=%d",
                icao24, callsign, lat, lon, baroAltitude, velocity, squawk, eventTime);
    }
}
