-- H3 hücresi bazında 1 dakikalık trafik özetleri.
-- H3 indeksi zaten uzamsal anahtar olduğundan PostGIS'e gerek yok.
CREATE TABLE IF NOT EXISTS cell_traffic (
    h3_cell        VARCHAR(16)      NOT NULL,   -- H3 res-5 hücresi (hex string)
    window_start   TIMESTAMPTZ      NOT NULL,
    window_end     TIMESTAMPTZ      NOT NULL,
    aircraft_count INTEGER          NOT NULL,
    avg_altitude_m DOUBLE PRECISION,
    avg_speed_ms   DOUBLE PRECISION,
    PRIMARY KEY (h3_cell, window_start)
);

CREATE INDEX IF NOT EXISTS idx_cell_traffic_window ON cell_traffic (window_start DESC);

-- Anomali ve yakınlık uyarıları
CREATE TABLE IF NOT EXISTS alerts (
    id           BIGSERIAL    PRIMARY KEY,
    alert_type   VARCHAR(32)  NOT NULL,   -- RAPID_DESCENT, EMERGENCY_SQUAWK, SPEED_JUMP, PROXIMITY
    icao24       VARCHAR(8)   NOT NULL,
    other_icao24 VARCHAR(8),              -- yalnızca PROXIMITY için
    callsign     VARCHAR(16),
    h3_cell      VARCHAR(16),
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION,
    altitude_m   DOUBLE PRECISION,
    details      TEXT,
    event_time   TIMESTAMPTZ  NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_event_time ON alerts (event_time DESC);
