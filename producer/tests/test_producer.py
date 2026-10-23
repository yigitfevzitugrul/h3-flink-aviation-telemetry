import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opensky_client import parse_states  # noqa: E402
from simulator import Simulator  # noqa: E402

REQUIRED = {"icao24", "callsign", "lat", "lon", "baro_altitude", "velocity", "heading",
            "vertical_rate", "squawk", "on_ground", "event_time", "source"}


def test_simulator_emits_one_valid_record_per_aircraft():
    sim = Simulator(n_aircraft=20, seed=42)
    records = sim.tick(1_700_000_000_000)
    assert len(records) == 20
    for r in records:
        d = json.loads(r.to_json())
        assert set(d) == REQUIRED
        assert len(d["icao24"]) == 6
        assert d["event_time"] == 1_700_000_000_000


def test_simulator_respawns_finished_flights():
    sim = Simulator(n_aircraft=10, seed=1)
    first = {f.icao24 for f in sim.flights}
    for i in range(2000):   # ~2.8 saatlik simülasyon: her uçuş en az bir kez biter
        sim.tick(i * 5000)
    assert first.isdisjoint({f.icao24 for f in sim.flights})


def test_normal_flight_profile_never_looks_like_rapid_descent():
    import simulator
    simulator.P_RAPID_DESCENT = 0.0   # bilinçli anomalileri kapat
    try:
        sim = Simulator(n_aircraft=40, seed=7)
        for i in range(1500):
            for r in sim.tick(i * 5000):
                assert r.vertical_rate > -30.48, r   # -6000 ft/dk eşiği
    finally:
        simulator.P_RAPID_DESCENT = 0.0005


def test_parse_states_skips_missing_positions_and_strips_callsign():
    payload = {"time": 1, "states": [
        ["4ba9c1", "THY1AB  ", "Turkey", 1700000000, 1700000001, 29.0, 41.0, 10000.0,
         False, 230.0, 90.0, 0.0, None, 10100.0, "1234", False, 0],
        ["4ba9c2", None, "Turkey", None, 1700000005, None, None, None,
         True, 0.0, 0.0, None, None, None, None, False, 0],
    ]}
    out = parse_states(payload)
    assert len(out) == 1
    p = out[0]
    assert p.callsign == "THY1AB"
    assert (p.lat, p.lon) == (41.0, 29.0)
    assert p.event_time == 1_700_000_000_000
    assert p.source == "opensky"


def test_parse_states_handles_empty_payload():
    assert parse_states({"time": 1, "states": None}) == []
