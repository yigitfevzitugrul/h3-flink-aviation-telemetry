"""Kafka'ya yazılan ortak uçak konum mesajı.

Birimler OpenSky ile aynıdır (SI): irtifa metre, hız m/s, dikey hız m/s,
event_time epoch milisaniye.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Optional


@dataclass
class Position:
    icao24: str
    callsign: Optional[str]
    lat: float
    lon: float
    baro_altitude: Optional[float]
    velocity: Optional[float]
    heading: Optional[float]
    vertical_rate: Optional[float]
    squawk: Optional[str]
    on_ground: bool
    event_time: int
    source: str

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode("utf-8")
