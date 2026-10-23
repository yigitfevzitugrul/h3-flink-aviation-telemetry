"""Uçak konumlarını (simülatör veya OpenSky) Kafka'ya yazan producer.

Mesaj anahtarı icao24'tür; böylece aynı uçağın mesajları aynı partisyona düşer
ve Flink tarafında uçak bazında sıralı işlenir.
"""
from __future__ import annotations

import logging
import os
import signal
import time

from confluent_kafka import Producer

from opensky_client import OpenSkyClient, RateLimited
from simulator import Simulator

log = logging.getLogger("producer")

running = True


def _stop(*_):
    global running
    running = False


def env(name: str, default: str) -> str:
    return os.environ.get(name) or default


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": env("KAFKA_BOOTSTRAP", "localhost:9094"),
        "enable.idempotence": True,
        "linger.ms": 50,
        "compression.type": "lz4",
    })


def send(producer: Producer, topic: str, positions) -> None:
    for p in positions:
        producer.produce(topic, key=p.icao24.encode(), value=p.to_json(), on_delivery=_on_delivery)
    producer.poll(0)


def _on_delivery(err, msg):
    if err is not None:
        log.error("Mesaj gönderilemedi: %s", err)


def run_sim(producer: Producer, topic: str) -> None:
    tick = float(env("SIM_TICK_SECONDS", "5"))
    sim = Simulator(n_aircraft=int(env("SIM_AIRCRAFT", "60")), tick_seconds=tick)
    log.info("Simülatör başladı: %d uçak, %.0f sn aralık", sim.n_aircraft, tick)
    while running:
        started = time.time()
        positions = sim.tick(int(started * 1000))
        send(producer, topic, positions)
        log.info("%d konum gönderildi", len(positions))
        time.sleep(max(0.0, tick - (time.time() - started)))


def run_opensky(producer: Producer, topic: str) -> None:
    bbox = [float(x) for x in env("BBOX", "35.8,25.6,42.2,44.8").split(",")]
    client = OpenSkyClient(bbox, os.environ.get("OPENSKY_CLIENT_ID"), os.environ.get("OPENSKY_CLIENT_SECRET"))
    # Türkiye bbox'ı ~3 kredi/istek: kimlikli hesap (4000/gün) için ~70 sn, anonim (400/gün) için ~11 dk
    interval = float(env("POLL_INTERVAL", "70" if client.authenticated else "660"))
    last_seen: dict[str, int] = {}
    log.info("OpenSky modu (%s), %.0f sn aralık", "kimlikli" if client.authenticated else "anonim", interval)

    while running:
        try:
            positions = client.fetch_states()
            fresh = [p for p in positions if last_seen.get(p.icao24) != p.event_time]
            last_seen = {p.icao24: p.event_time for p in positions}
            send(producer, topic, fresh)
            log.info("%d uçak alındı, %d yeni konum gönderildi", len(positions), len(fresh))
            wait = interval
        except RateLimited as e:
            log.warning(str(e))
            wait = e.retry_after
        except Exception as e:   # ağ hatası vb.: servis düşmesin, biraz bekleyip tekrar dene
            log.error("OpenSky isteği başarısız: %s", e)
            wait = min(interval, 60)
        _sleep(wait)


def _sleep(seconds: float) -> None:
    end = time.time() + seconds
    while running and time.time() < end:
        time.sleep(min(1.0, end - time.time()))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    topic = env("KAFKA_TOPIC", "aircraft-positions")
    producer = build_producer()
    mode = env("MODE", "sim").lower()
    try:
        if mode == "opensky":
            run_opensky(producer, topic)
        else:
            run_sim(producer, topic)
    finally:
        producer.flush(10)
        log.info("Producer kapandı")


if __name__ == "__main__":
    main()
