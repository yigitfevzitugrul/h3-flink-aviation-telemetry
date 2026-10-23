package com.aerotelemetry.sink;

import com.aerotelemetry.model.Alert;
import com.aerotelemetry.model.CellStats;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * PostgreSQL sink'leri.
 *
 * <p>Sink at-least-once çalışır; upsert (ON CONFLICT) sayesinde yeniden oynatılan kayıtlar
 * aynı satırın üzerine yazılır, yani sonuç idempotent'tir.
 */
public final class JdbcSinks {

    private static final String UPSERT_CELL_TRAFFIC =
            "INSERT INTO cell_traffic (h3_cell, window_start, window_end, aircraft_count, avg_altitude_m, avg_speed_ms) "
                    + "VALUES (?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT (h3_cell, window_start) DO UPDATE SET "
                    + "window_end = EXCLUDED.window_end, aircraft_count = EXCLUDED.aircraft_count, "
                    + "avg_altitude_m = EXCLUDED.avg_altitude_m, avg_speed_ms = EXCLUDED.avg_speed_ms";

    // Tablodaki tekillik kısıtı sayesinde yeniden oynatılan uyarılar sessizce atlanır
    private static final String INSERT_ALERT =
            "INSERT INTO alerts (alert_type, icao24, other_icao24, callsign, h3_cell, lat, lon, altitude_m, details, event_time) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT ON CONSTRAINT uq_alerts_event DO NOTHING";

    private JdbcSinks() {}

    public static JdbcConnectionOptions connection(String url, String user, String password) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withDriverName("org.postgresql.Driver")
                .withUsername(user)
                .withPassword(password)
                .build();
    }

    public static JdbcSink<CellStats> cellTraffic(JdbcConnectionOptions conn) {
        return JdbcSink.<CellStats>builder()
                .withQueryStatement(UPSERT_CELL_TRAFFIC, JdbcSinks::bindCellStats)
                .withExecutionOptions(executionOptions())
                .buildAtLeastOnce(conn);
    }

    private static void bindCellStats(PreparedStatement ps, CellStats s) throws SQLException {
        ps.setString(1, s.h3Cell);
        ps.setObject(2, utc(s.windowStart));
        ps.setObject(3, utc(s.windowEnd));
        ps.setInt(4, s.aircraftCount);
        setNullableDouble(ps, 5, s.avgAltitudeM);
        setNullableDouble(ps, 6, s.avgSpeedMs);
    }

    public static JdbcSink<Alert> alerts(JdbcConnectionOptions conn) {
        return JdbcSink.<Alert>builder()
                .withQueryStatement(INSERT_ALERT, JdbcSinks::bindAlert)
                .withExecutionOptions(executionOptions())
                .buildAtLeastOnce(conn);
    }

    private static void bindAlert(PreparedStatement ps, Alert a) throws SQLException {
        ps.setString(1, a.alertType);
        ps.setString(2, a.icao24);
        ps.setString(3, a.otherIcao24);
        ps.setString(4, a.callsign);
        ps.setString(5, a.h3Cell);
        ps.setDouble(6, a.lat);
        ps.setDouble(7, a.lon);
        setNullableDouble(ps, 8, a.altitudeM);
        ps.setString(9, a.details);
        ps.setObject(10, utc(a.eventTime));
    }

    static JdbcExecutionOptions executionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(500)
                .withBatchIntervalMs(2_000)
                .withMaxRetries(3)
                .build();
    }

    static OffsetDateTime utc(long epochMillis) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }

    static void setNullableDouble(PreparedStatement ps, int index, Double value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.DOUBLE);
        } else {
            ps.setDouble(index, value);
        }
    }
}
