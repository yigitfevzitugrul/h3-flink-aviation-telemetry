package com.aerotelemetry.model;

/** Anomali veya yakınlık uyarısı. */
public class Alert {

    public static final String RAPID_DESCENT = "RAPID_DESCENT";
    public static final String EMERGENCY_SQUAWK = "EMERGENCY_SQUAWK";
    public static final String SPEED_JUMP = "SPEED_JUMP";
    public static final String PROXIMITY = "PROXIMITY";

    public String alertType;
    public String icao24;
    /** Yalnızca PROXIMITY için; çiftte alfabetik olarak büyük olan uçak. */
    public String otherIcao24;
    public String callsign;
    public String h3Cell;
    public double lat;
    public double lon;
    public Double altitudeM;
    public String details;
    public long eventTime;

    public Alert() {}

    public static Alert of(String type, AircraftPosition p, String details) {
        Alert a = new Alert();
        a.alertType = type;
        a.icao24 = p.icao24;
        a.callsign = p.callsign;
        a.h3Cell = p.h3Region;
        a.lat = p.lat;
        a.lon = p.lon;
        a.altitudeM = p.baroAltitude;
        a.details = details;
        a.eventTime = p.eventTime;
        return a;
    }

    @Override
    public String toString() {
        return String.format("%s %s%s @%d: %s", alertType, icao24,
                otherIcao24 == null ? "" : "/" + otherIcao24, eventTime, details);
    }
}
