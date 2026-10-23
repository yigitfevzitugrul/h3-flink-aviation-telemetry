"""OpenSky Network REST API istemcisi (/states/all).

Kimlik bilgisi verilirse OAuth2 client-credentials akışıyla token alınır,
verilmezse anonim (düşük kotalı) erişim kullanılır.
"""
from __future__ import annotations

import logging
import time
from typing import List, Optional, Sequence

import requests

from schema import Position

log = logging.getLogger(__name__)

API_URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"


class RateLimited(Exception):
    def __init__(self, retry_after: float):
        super().__init__(f"OpenSky kota sınırı, {retry_after:.0f} sn sonra tekrar denenecek")
        self.retry_after = retry_after


def parse_states(payload: dict) -> List[Position]:
    """OpenSky state vector dizilerini Position nesnelerine çevirir.

    Alan sırası: 0 icao24, 1 callsign, 3 time_position, 4 last_contact, 5 lon, 6 lat,
    7 baro_altitude, 8 on_ground, 9 velocity, 10 true_track, 11 vertical_rate, 14 squawk
    """
    out: List[Position] = []
    for s in payload.get("states") or []:
        lon, lat = s[5], s[6]
        if lat is None or lon is None:
            continue
        ts = s[3] if s[3] is not None else s[4]
        callsign = (s[1] or "").strip() or None
        out.append(Position(
            icao24=s[0],
            callsign=callsign,
            lat=lat,
            lon=lon,
            baro_altitude=s[7],
            velocity=s[9],
            heading=s[10],
            vertical_rate=s[11],
            squawk=s[14],
            on_ground=bool(s[8]),
            event_time=int(ts * 1000),
            source="opensky",
        ))
    return out


class OpenSkyClient:
    def __init__(self, bbox: Sequence[float], client_id: Optional[str] = None,
                 client_secret: Optional[str] = None, timeout: float = 20.0):
        self.lamin, self.lomin, self.lamax, self.lomax = bbox
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self.session = requests.Session()
        self._token: Optional[str] = None
        self._token_expiry = 0.0

    @property
    def authenticated(self) -> bool:
        return bool(self.client_id and self.client_secret)

    def _auth_header(self) -> dict:
        if not self.authenticated:
            return {}
        if self._token is None or time.time() >= self._token_expiry:
            resp = self.session.post(TOKEN_URL, data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }, timeout=self.timeout)
            resp.raise_for_status()
            body = resp.json()
            self._token = body["access_token"]
            self._token_expiry = time.time() + body.get("expires_in", 1800) - 60
            log.info("OpenSky token alındı")
        return {"Authorization": f"Bearer {self._token}"}

    def fetch_states(self) -> List[Position]:
        params = {"lamin": self.lamin, "lomin": self.lomin, "lamax": self.lamax, "lomax": self.lomax}
        resp = self.session.get(API_URL, params=params, headers=self._auth_header(), timeout=self.timeout)
        if resp.status_code == 429:
            retry = float(resp.headers.get("X-Rate-Limit-Retry-After-Seconds", 300))
            raise RateLimited(retry)
        if resp.status_code == 401 and self.authenticated:
            self._token = None   # token süresi dolmuş olabilir; bir sonraki turda yenilenir
        resp.raise_for_status()
        return parse_states(resp.json())
