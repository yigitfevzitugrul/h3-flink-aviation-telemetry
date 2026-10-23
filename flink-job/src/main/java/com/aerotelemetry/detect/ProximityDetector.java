package com.aerotelemetry.detect;

import com.aerotelemetry.model.AircraftPosition;
import com.aerotelemetry.model.Alert;
import com.aerotelemetry.model.CellProbe;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * H3 hücresi bazında (keyBy targetCell) yakın uçuş tespiti.
 *
 * <p>Her hücre yalnızca kendi içindeki uçakların ("sakinler") son konumunu saklar. Gelen her
 * probe (sakin ya da komşu hücreden) sakinlerle karşılaştırılır. Aynı çiftin iki hücrede birden
 * raporlanmaması için uyarı yalnızca iki uçağın ev hücrelerinden alfabetik olarak küçük olanında üretilir.
 *
 * <p>Eşikler demo amaçlıdır (gerçek radar ayırma minimumu 5 NM / 1000 ft); res-7 hücre ve
 * gridDisk(1) ile 1 km yatay mesafe eksiksiz kapsanır.
 */
public class ProximityDetector extends KeyedProcessFunction<String, CellProbe, Alert> {

    static final double MAX_HORIZONTAL_M = 1_000.0;
    static final double MAX_VERTICAL_M = 305.0;               // ~1000 ft
    static final long MAX_TIME_SKEW_MS = 10_000;              // karşılaştırılan konumlar arası en fazla fark
    static final long RESIDENT_EXPIRY_MS = 30_000;

    private transient MapState<String, AircraftPosition> residents;

    @Override
    public void open(OpenContext openContext) {
        MapStateDescriptor<String, AircraftPosition> desc =
                new MapStateDescriptor<>("residents", String.class, AircraftPosition.class);
        desc.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofMinutes(5)).build());
        residents = getRuntimeContext().getMapState(desc);
    }

    @Override
    public void processElement(CellProbe probe, Context ctx, Collector<Alert> out) throws Exception {
        String cell = ctx.getCurrentKey();
        AircraftPosition p = probe.position;
        boolean reportHere = cell.compareTo(p.h3Proximity) <= 0;
        List<String> expired = new ArrayList<>();

        for (Map.Entry<String, AircraftPosition> e : residents.entries()) {
            AircraftPosition other = e.getValue();
            if (other.eventTime < p.eventTime - RESIDENT_EXPIRY_MS) {
                expired.add(e.getKey());
                continue;
            }
            if (!reportHere || other.icao24.equals(p.icao24)
                    || Math.abs(other.eventTime - p.eventTime) > MAX_TIME_SKEW_MS) {
                continue;
            }
            double vertical = Math.abs(other.baroAltitude - p.baroAltitude);
            if (vertical > MAX_VERTICAL_M) {
                continue;
            }
            double horizontal = Geo.haversineM(p.lat, p.lon, other.lat, other.lon);
            if (horizontal <= MAX_HORIZONTAL_M) {
                out.collect(pairAlert(p, other, horizontal, vertical));
            }
        }

        for (String id : expired) {
            residents.remove(id);
        }
        if (probe.home) {
            residents.put(p.icao24, p);
        } else if (residents.contains(p.icao24)) {
            residents.remove(p.icao24);   // uçak bu hücreden çıkmış
        }
    }

    private static Alert pairAlert(AircraftPosition p, AircraftPosition other, double horizontal, double vertical) {
        // Çift anahtarı sıradan bağımsız olsun: icao24 küçük olan ana uçak
        AircraftPosition first = p.icao24.compareTo(other.icao24) < 0 ? p : other;
        AircraftPosition second = first == p ? other : p;
        Alert a = Alert.of(Alert.PROXIMITY, first, String.format(
                "%s ile yatay %.0f m, dikey %.0f m (%.0f ft)",
                second.callsign != null ? second.callsign : second.icao24,
                horizontal, vertical, Geo.metersToFeet(vertical)));
        a.otherIcao24 = second.icao24;
        a.eventTime = Math.max(p.eventTime, other.eventTime);
        return a;
    }
}
