package com.aerotelemetry.model;

/** Bir H3 hücresinin tek bir zaman penceresindeki trafik özeti. */
public class CellStats {

    public String h3Cell;
    public long windowStart;
    public long windowEnd;
    public int aircraftCount;
    public Double avgAltitudeM;
    public Double avgSpeedMs;

    public CellStats() {}

    @Override
    public String toString() {
        return String.format("%s [%d-%d] n=%d alt=%s v=%s",
                h3Cell, windowStart, windowEnd, aircraftCount, avgAltitudeM, avgSpeedMs);
    }
}
