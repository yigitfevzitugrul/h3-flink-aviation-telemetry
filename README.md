# ✈️ H3 + Apache Flink ile Gerçek Zamanlı Havacılık Telemetrisi

Canlı uçak konum verisini (OpenSky Network veya sentetik simülatör) **Kafka** üzerinden akıtan,
**Apache Flink** ile gerçek zamanlı işleyen ve konumları **Uber H3** altıgen hücrelerine indeksleyerek
hava trafiği yoğunluğunu ve anomalileri bir harita üzerinde gösteren uçtan uca bir veri akışı projesi.

> 🚧 Geliştirme aşamasında.

## Mimari

```
[OpenSky API] ─┐
               ├─> producer (Python) ──> Kafka: aircraft-positions
[Simülatör]  ──┘                               │
                                               ▼
                                    Flink Job (Java 17)
                    ┌──────────────────────┼─────────────────────────┐
                    ▼                      ▼                         ▼
        H3 hücre yoğunluğu         Uçak bazlı anomali          Yakınlık uyarısı
        (1 dk tumbling window)     (keyed state)               (H3 gridDisk)
                    └────────> PostgreSQL (cell_traffic, alerts) <────┘
                                               │
                                     FastAPI ──> Web harita (deck.gl)
```

## Teknolojiler

| Katman | Teknoloji |
|---|---|
| Mesajlaşma | Apache Kafka 3.9 (KRaft) |
| Akış işleme | Apache Flink 1.20 (Java 17) |
| Uzamsal indeks | Uber H3 |
| Depolama | PostgreSQL 16 |
| API / Görselleştirme | FastAPI, deck.gl, MapLibre |
| Altyapı | Docker Compose |

## Hızlı Başlangıç

```bash
cp .env.example .env
docker compose up -d
```

| Servis | Adres |
|---|---|
| Flink Web UI | http://localhost:8081 |
| Kafka (host) | localhost:9094 |
| PostgreSQL | localhost:5432 (telemetry / telemetry) |
