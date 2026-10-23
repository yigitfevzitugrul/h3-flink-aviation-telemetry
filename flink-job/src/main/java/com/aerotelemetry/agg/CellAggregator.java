package com.aerotelemetry.agg;

import com.aerotelemetry.model.AircraftPosition;
import com.aerotelemetry.model.CellStats;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * Pencere içindeki konumları artımlı (incremental) olarak toplar: pencere boyunca tüm
 * kayıtları tutmak yerine yalnızca küçük bir accumulator saklanır.
 *
 * <p>Uçak sayısı farklı icao24 sayısıdır; bir uçak pencerede birden çok mesaj gönderebilir.
 */
public class CellAggregator implements AggregateFunction<AircraftPosition, CellAggregator.Acc, CellStats> {

    public static class Acc {
        public HashSet<String> aircraft = new HashSet<>();
        public double altitudeSum;
        public long altitudeCount;
        public double speedSum;
        public long speedCount;
    }

    @Override
    public Acc createAccumulator() {
        return new Acc();
    }

    @Override
    public Acc add(AircraftPosition p, Acc acc) {
        acc.aircraft.add(p.icao24);
        if (p.baroAltitude != null) {
            acc.altitudeSum += p.baroAltitude;
            acc.altitudeCount++;
        }
        if (p.velocity != null) {
            acc.speedSum += p.velocity;
            acc.speedCount++;
        }
        return acc;
    }

    @Override
    public CellStats getResult(Acc acc) {
        CellStats s = new CellStats();
        s.aircraftCount = acc.aircraft.size();
        s.avgAltitudeM = acc.altitudeCount == 0 ? null : acc.altitudeSum / acc.altitudeCount;
        s.avgSpeedMs = acc.speedCount == 0 ? null : acc.speedSum / acc.speedCount;
        return s;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        a.aircraft.addAll(b.aircraft);
        a.altitudeSum += b.altitudeSum;
        a.altitudeCount += b.altitudeCount;
        a.speedSum += b.speedSum;
        a.speedCount += b.speedCount;
        return a;
    }

    /** Toplanan sonuca hücre anahtarını ve pencere sınırlarını ekler. */
    public static class AttachWindow extends ProcessWindowFunction<CellStats, CellStats, String, TimeWindow> {
        @Override
        public void process(String cell, Context ctx, Iterable<CellStats> results, Collector<CellStats> out) {
            CellStats s = results.iterator().next();
            s.h3Cell = cell;
            s.windowStart = ctx.window().getStart();
            s.windowEnd = ctx.window().getEnd();
            out.collect(s);
        }
    }
}
