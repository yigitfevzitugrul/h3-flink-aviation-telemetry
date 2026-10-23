"""Türkiye üzerindeki havalimanları arasında uçan sentetik bir filo.

Flink tarafındaki tespitleri sınayabilmek için ara sıra bilinçli anomaliler üretir:
  * RAPID_DESCENT  : seyir sırasında ~-45 m/s (~-8900 ft/dk) ani alçalma
  * EMERGENCY      : 7700 / 7600 / 7500 squawk kodu
  * GPS_GLITCH     : tek bir mesajda konumun yüzlerce km sıçraması
  * SHADOW         : başka bir uçağın hemen arkasında, yakın irtifada uçan "gölge" uçak
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import List, Optional

from schema import Position

AIRPORTS = {
    "IST": (41.2753, 28.7519), "SAW": (40.8986, 29.3092), "ESB": (40.1281, 32.9951),
    "ADB": (38.2924, 27.1570), "AYT": (36.8987, 30.8005), "ADA": (36.9822, 35.2804),
    "TZX": (40.9951, 39.7897), "DLM": (36.7131, 28.7925), "BJV": (37.2506, 27.6643),
    "GZT": (36.9472, 37.4787), "ERZ": (39.9565, 41.1702), "VAN": (38.4682, 43.3323),
    "DIY": (37.8939, 40.2010), "SZF": (41.2545, 36.5671),
}
AIRLINES = ["THY", "PGT", "SXS", "AJA", "KKK"]
# Az sayıda uçuş seviyesi kullanıyoruz ki karşılaşmalar gerçekten aynı irtifada olsun
CRUISE_LEVELS_M = [10668.0, 11278.0]   # FL350, FL370
CRUISE_SPEED_MS = 230.0
TERMINAL_SPEED_MS = 140.0
# İrtifa, kat edilen/kalan mesafeyle sınırlanır: tırmanış ~%8, alçalış 3° süzülüş (~%5.24).
# Böylece normal dikey hızlar gerçekçi kalır (~ +11 / -7 m/s) ve anomali eşiğine takılmaz.
CLIMB_GRADIENT = 0.08
DESCENT_GRADIENT = 0.0524

P_RAPID_DESCENT = 0.0005
P_EMERGENCY = 0.0002
P_GPS_GLITCH = 0.0003
P_SHADOW_SPAWN = 0.08


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def bearing_deg(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


@dataclass
class Flight:
    icao24: str
    callsign: str
    origin: str
    dest: str
    cruise_alt: float
    progress: float = 0.0               # 0..1 rota üzerindeki konum
    altitude: float = 0.0
    squawk: str = "2000"
    descent_ticks: int = 0              # >0 ise ani alçalma anomalisi sürüyor
    alt_offset: float = 0.0             # gölge uçaklar için irtifa farkı
    distance_m: float = field(init=False)

    def __post_init__(self):
        o, d = AIRPORTS[self.origin], AIRPORTS[self.dest]
        self.distance_m = haversine_m(*o, *d)
        self.altitude = self._profile_altitude()

    @property
    def done(self) -> bool:
        return self.progress >= 1.0

    def latlon(self):
        (la1, lo1), (la2, lo2) = AIRPORTS[self.origin], AIRPORTS[self.dest]
        f = min(self.progress, 1.0)
        return la1 + (la2 - la1) * f, lo1 + (lo2 - lo1) * f

    def _base_altitude(self) -> float:
        flown = min(self.progress, 1.0) * self.distance_m
        remaining = self.distance_m - flown
        return min(self.cruise_alt, flown * CLIMB_GRADIENT, remaining * DESCENT_GRADIENT)

    def _profile_altitude(self) -> float:
        return max(0.0, self._base_altitude() + self.alt_offset)

    def in_cruise(self) -> bool:
        return self._base_altitude() >= self.cruise_alt - 1.0


class Simulator:
    def __init__(self, n_aircraft: int = 60, tick_seconds: float = 5.0, seed: Optional[int] = None):
        self.rng = random.Random(seed)
        self.tick_seconds = tick_seconds
        self.n_aircraft = n_aircraft
        self.flights: List[Flight] = []
        for _ in range(n_aircraft):
            f = self._new_flight()
            f.progress = self.rng.uniform(0.0, 0.9)   # filo başlangıçta havada olsun
            f.altitude = f._profile_altitude()
            self.flights.append(f)

    def _new_icao24(self) -> str:
        return f"{self.rng.randrange(0x4B0000, 0x4BFFFF):06x}"   # 4B.... Türkiye ICAO bloğu

    def _new_flight(self) -> Flight:
        origin, dest = self.rng.sample(sorted(AIRPORTS), 2)
        return Flight(
            icao24=self._new_icao24(),
            callsign=f"{self.rng.choice(AIRLINES)}{self.rng.randint(100, 9999)}",
            origin=origin,
            dest=dest,
            cruise_alt=self.rng.choice(CRUISE_LEVELS_M),
        )

    def _new_shadow(self) -> Optional[Flight]:
        leaders = [f for f in self.flights if f.in_cruise() and f.progress < 0.6]
        if not leaders:
            return None
        lead = self.rng.choice(leaders)
        shadow = self._new_flight()
        shadow.origin, shadow.dest, shadow.cruise_alt = lead.origin, lead.dest, lead.cruise_alt
        shadow.__post_init__()
        shadow.progress = max(0.0, lead.progress - 600.0 / lead.distance_m)     # ~600 m arkada
        shadow.alt_offset = self.rng.uniform(-120.0, 120.0)                     # < 400 ft fark
        shadow.altitude = shadow._profile_altitude()
        return shadow

    def _respawn(self) -> Flight:
        if self.rng.random() < P_SHADOW_SPAWN:
            shadow = self._new_shadow()
            if shadow:
                return shadow
        return self._new_flight()

    def tick(self, now_ms: int) -> List[Position]:
        """Filoyu bir adım ilerletir ve her uçak için bir konum mesajı döner."""
        out: List[Position] = []
        dt = self.tick_seconds
        for i, f in enumerate(self.flights):
            if f.done:
                f = self._respawn()
                self.flights[i] = f

            # Hız irtifayla birlikte artar: alçakta yaklaşma hızı, seyir irtifasında seyir hızı
            climb_ratio = min(1.0, f.altitude / f.cruise_alt) if f.cruise_alt else 1.0
            speed = TERMINAL_SPEED_MS + (CRUISE_SPEED_MS - TERMINAL_SPEED_MS) * climb_ratio
            prev_alt = f.altitude
            f.progress += speed * dt / f.distance_m

            if f.descent_ticks > 0:
                f.altitude = max(1500.0, f.altitude - 45.0 * dt)
                f.descent_ticks -= 1
                if f.descent_ticks == 0:   # yeni irtifada seyre devam et
                    f.cruise_alt, f.alt_offset = f.altitude, 0.0
            else:
                f.altitude = f._profile_altitude()
                if f.in_cruise() and self.rng.random() < P_RAPID_DESCENT:
                    f.descent_ticks = 6

            if f.squawk == "2000" and self.rng.random() < P_EMERGENCY:
                f.squawk = self.rng.choices(["7700", "7600", "7500"], weights=[6, 3, 1])[0]

            lat, lon = f.latlon()
            if self.rng.random() < P_GPS_GLITCH:   # sadece bu mesajda bozuk konum
                lat += self.rng.uniform(-3, 3)
                lon += self.rng.uniform(-3, 3)

            (la2, lo2) = AIRPORTS[f.dest]
            out.append(Position(
                icao24=f.icao24,
                callsign=f.callsign,
                lat=round(lat, 5),
                lon=round(lon, 5),
                baro_altitude=round(f.altitude, 1),
                velocity=round(speed, 1),
                heading=round(bearing_deg(lat, lon, la2, lo2), 1),
                vertical_rate=round((f.altitude - prev_alt) / dt, 2),
                squawk=f.squawk,
                on_ground=f.altitude < 1.0,
                event_time=now_ms,
                source="sim",
            ))
        return out
