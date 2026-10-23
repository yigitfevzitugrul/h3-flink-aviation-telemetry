import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main  # noqa: E402


class FakeDb:
    """main.fetch yerine geçer: çalıştırılan sorguları kaydeder, SQL'e göre hazır cevap döner."""

    def __init__(self):
        self.calls = []

    async def __call__(self, sql, params=None):
        self.calls.append((sql, params))
        if "FROM cell_traffic, latest" in sql:
            return [{"h3_cell": "852d1943fffffff", "aircraft": 2, "avg_altitude_m": 10696.0, "avg_speed_ms": 230.0}]
        if "max(window_start) AS ws" in sql:
            return [{"ws": "2026-10-23T16:05:00+00:00", "we": "2026-10-23T16:06:00+00:00"}]
        if "FROM alerts" in sql and "GROUP BY alert_type" in sql:
            return [{"alert_type": "PROXIMITY", "n": 2}, {"alert_type": "SPEED_JUMP", "n": 5}]
        if "FROM alerts" in sql:
            return [{"id": 1, "alert_type": "SPEED_JUMP", "icao24": "4ba8f2"}]
        if "GROUP BY window_start" in sql:
            return [{"window_start": "2026-10-23T16:05:00+00:00", "active_cells": 95, "busiest_cell": 2}]
        return [{"?column?": 1}]


@pytest.fixture
def db(monkeypatch):
    fake = FakeDb()
    monkeypatch.setattr(main, "fetch", fake)
    return fake


@pytest.fixture
def client():
    # `with` kullanılmıyor: lifespan (gerçek bağlantı havuzu) çalışmaz
    return TestClient(main.app)


def test_health(client, db):
    assert client.get("/api/health").json() == {"status": "ok"}


def test_cells_returns_latest_window_and_passes_minutes(client, db):
    body = client.get("/api/cells?minutes=5").json()
    assert body["window_start"] == "2026-10-23T16:05:00+00:00"
    assert body["cells"][0]["h3_cell"] == "852d1943fffffff"
    assert db.calls[0][1] == {"minutes": 5}


@pytest.mark.parametrize("minutes", [0, 61])
def test_cells_rejects_out_of_range_minutes(client, db, minutes):
    assert client.get(f"/api/cells?minutes={minutes}").status_code == 422
    assert db.calls == []


def test_alerts_passes_filters(client, db):
    body = client.get("/api/alerts?limit=10&since_minutes=30&type=SPEED_JUMP").json()
    assert body[0]["alert_type"] == "SPEED_JUMP"
    assert db.calls[0][1] == {"limit": 10, "since": 30, "type": "SPEED_JUMP"}


def test_alerts_defaults_to_all_types(client, db):
    client.get("/api/alerts")
    assert db.calls[0][1] == {"limit": 50, "since": 60, "type": None}


def test_stats_groups_alert_counts_by_type(client, db):
    body = client.get("/api/stats").json()
    assert body["alerts_last_hour"] == {"PROXIMITY": 2, "SPEED_JUMP": 5}
    assert body["series"][0]["active_cells"] == 95


def test_serves_web_ui(client):
    res = client.get("/")
    assert res.status_code == 200
    assert "<title>Aero Telemetry</title>" in res.text
