"""Dashboard API'si: Flink'in PostgreSQL'e yazdığı sonuçları sunar ve web haritasını servis eder."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://telemetry:telemetry@localhost:5432/telemetry")
WEB_DIR = os.environ.get("WEB_DIR", os.path.join(os.path.dirname(__file__), "..", "web"))

pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=5, open=False,
                           kwargs={"row_factory": dict_row})


@asynccontextmanager
async def lifespan(_: FastAPI):
    await pool.open()
    yield
    await pool.close()


app = FastAPI(title="Aero Telemetry API", lifespan=lifespan)

# Son yazılan pencereden geriye `minutes` dakika: hücre başına en yüksek uçak sayısı
CELLS_SQL = """
WITH latest AS (SELECT max(window_start) AS ws FROM cell_traffic)
SELECT h3_cell,
       max(aircraft_count)       AS aircraft,
       avg(avg_altitude_m)       AS avg_altitude_m,
       avg(avg_speed_ms)         AS avg_speed_ms
FROM cell_traffic, latest
WHERE window_start > latest.ws - make_interval(mins => %(minutes)s)
GROUP BY h3_cell
"""

ALERTS_SQL = """
SELECT id, alert_type, icao24, other_icao24, callsign, h3_cell, lat, lon, altitude_m, details, event_time
FROM alerts
WHERE (%(type)s::text IS NULL OR alert_type = %(type)s::text)
  AND event_time > now() - make_interval(mins => %(since)s)
ORDER BY event_time DESC
LIMIT %(limit)s
"""

SERIES_SQL = """
SELECT window_start, count(*) AS active_cells, max(aircraft_count) AS busiest_cell
FROM cell_traffic
WHERE window_start > (SELECT max(window_start) FROM cell_traffic) - interval '30 minutes'
GROUP BY window_start
ORDER BY window_start
"""

ALERT_COUNTS_SQL = """
SELECT alert_type, count(*) AS n
FROM alerts
WHERE event_time > now() - interval '1 hour'
GROUP BY alert_type
"""


async def fetch(sql: str, params: Optional[dict] = None) -> list[dict]:
    async with pool.connection() as conn:
        cur = await conn.execute(sql, params or {})
        return await cur.fetchall()


@app.get("/api/health")
async def health():
    await fetch("SELECT 1")
    return {"status": "ok"}


@app.get("/api/cells")
async def cells(minutes: int = Query(1, ge=1, le=60)):
    """H3 res-5 hücrelerinin son pencere(ler)deki trafik yoğunluğu."""
    rows = await fetch(CELLS_SQL, {"minutes": minutes})
    latest = await fetch("SELECT max(window_start) AS ws, max(window_end) AS we FROM cell_traffic")
    return {"window_start": latest[0]["ws"], "window_end": latest[0]["we"], "cells": rows}


@app.get("/api/alerts")
async def alerts(limit: int = Query(50, ge=1, le=500),
                 since_minutes: int = Query(60, ge=1, le=1440),
                 type: Optional[str] = None):
    return await fetch(ALERTS_SQL, {"limit": limit, "since": since_minutes, "type": type})


@app.get("/api/stats")
async def stats():
    series = await fetch(SERIES_SQL)
    counts = await fetch(ALERT_COUNTS_SQL)
    return {
        "series": series,
        "alerts_last_hour": {r["alert_type"]: r["n"] for r in counts},
    }


# API rotalarından sonra: geri kalan her şey statik web arayüzü
app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
