package com.aerotelemetry.detect;

import com.aerotelemetry.model.AircraftPosition;
import com.aerotelemetry.model.Alert;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

/**
 * Uçak bazında (keyBy icao24) durumlu anomali tespiti.
 *
 * <ul>
 *   <li>{@code SPEED_JUMP}: önceki konuma göre fiziksel olarak imkânsız yer hızı (GPS/ADS-B hatası).
 *       Hatalı nokta diğer kontrollere sokulmaz ve son konum olarak saklanmaz.</li>
 *   <li>{@code RAPID_DESCENT}: dikey hız -6000 ft/dk'nın altında; her alçalma bölümü için bir kez.</li>
 *   <li>{@code EMERGENCY_SQUAWK}: 7500/7600/7700 koduna geçiş anında bir kez.</li>
 * </ul>
 *
 * State TTL sayesinde menzilden çıkan uçakların durumu kendiliğinden temizlenir.
 */
public class AnomalyDetector extends KeyedProcessFunction<String, AircraftPosition, Alert> {

    static final double RAPID_DESCENT_MS = -30.48;          // -6000 ft/dk
    static final double MIN_DESCENT_ALTITUDE_M = 1_000.0;   // yaklaşma/iniş fazını hariç tut
    static final double MAX_PLAUSIBLE_SPEED_MS = 450.0;     // ~875 knot yer hızı
    static final int MAX_CONSECUTIVE_JUMPS = 3;             // sonra yeni konumu gerçek kabul et

    static final Map<String, String> EMERGENCY_CODES = Map.of(
            "7500", "kaçırma (hijack)",
            "7600", "telsiz arızası",
            "7700", "genel acil durum");

    /** Bir uçak için saklanan durum. */
    public static class Track {
        public AircraftPosition last;
        public int consecutiveJumps;
        public boolean descending;
        public String squawk;

        public Track() {}
    }

    private transient ValueState<Track> trackState;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Track> desc = new ValueStateDescriptor<>("track", Track.class);
        desc.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofMinutes(30)).build());
        trackState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(AircraftPosition p, Context ctx, Collector<Alert> out) throws Exception {
        Track track = trackState.value();
        if (track == null) {
            track = new Track();
        }
        AircraftPosition last = track.last;
        double dtSeconds = last == null ? 0 : (p.eventTime - last.eventTime) / 1000.0;

        if (last != null && dtSeconds <= 0) {
            return;   // tekrar eden veya sırası bozuk mesaj: durumu değiştirme
        }

        if (last != null) {
            double speed = Geo.haversineM(last.lat, last.lon, p.lat, p.lon) / dtSeconds;
            if (speed > MAX_PLAUSIBLE_SPEED_MS && track.consecutiveJumps < MAX_CONSECUTIVE_JUMPS) {
                if (track.consecutiveJumps == 0) {
                    out.collect(Alert.of(Alert.SPEED_JUMP, p,
                            String.format("%.0f sn'de %.0f km sıçrama (%.0f m/s)",
                                    dtSeconds, speed * dtSeconds / 1000, speed)));
                }
                track.consecutiveJumps++;
                trackState.update(track);
                return;
            }
        }
        track.consecutiveJumps = 0;

        Double verticalRate = p.verticalRate;
        if (verticalRate == null && last != null && last.baroAltitude != null && p.baroAltitude != null) {
            verticalRate = (p.baroAltitude - last.baroAltitude) / dtSeconds;
        }
        boolean rapidDescent = verticalRate != null && verticalRate < RAPID_DESCENT_MS
                && p.baroAltitude != null && p.baroAltitude > MIN_DESCENT_ALTITUDE_M;
        if (rapidDescent && !track.descending) {
            out.collect(Alert.of(Alert.RAPID_DESCENT, p,
                    String.format("dikey hız %.1f m/s (%.0f ft/dk)",
                            verticalRate, Geo.metersToFeet(verticalRate) * 60)));
        }
        track.descending = rapidDescent;

        String meaning = p.squawk == null ? null : EMERGENCY_CODES.get(p.squawk);
        if (meaning != null && !p.squawk.equals(track.squawk)) {
            out.collect(Alert.of(Alert.EMERGENCY_SQUAWK, p, "squawk " + p.squawk + ": " + meaning));
        }
        track.squawk = p.squawk;

        track.last = p;
        trackState.update(track);
    }
}
