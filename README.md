# twin-pipeline

A small real-time **digital-twin pipeline** in Go: simulated sensors publish over MQTT,
a concurrent server ingests them into an in-memory state store, and (soon) streams the
live state to a dashboard.

```
┌───────────┐  twin/sensors/{asset}/{metric}  ┌────────────┐   ┌──────────────┐   ┌─────────────┐
│ simulator │ ───────── MQTT (QoS 1) ───────▶ │  mosquitto │──▶│ server       │──▶│ state.Store │
└───────────┘                                 └────────────┘   │ worker pool  │   │ (RWMutex)   │
   N assets × 7 metrics                                        │ → ingest     │   └─────────────┘
                                                               └──────────────┘
```

## Quick start

```bash
make broker          # mosquitto in Docker
make server          # terminal 1   (hot reload via `air`; or: go run ./cmd/server)
make simulator       # terminal 2   (or: ASSETS=50 go run ./cmd/simulator)
```

Everything in containers: `make up`.

## Configuration (environment variables)

| Variable | Used by | Default | Meaning |
|---|---|---|---|
| `ENV` | both | `development` | `production` → JSON logs |
| `MQTT_BROKER_URL` | both | `mqtt://127.0.0.1:1883` | Broker address |
| `MQTT_CLIENT_ID` | both | `twin-server` / `simulator` | Must be unique per running client |
| `WORKERS` | server | `10` | Worker goroutines |
| `QUEUE_SIZE` | server | `1000` | Buffered jobs before backpressure |
| `MAX_ASSETS` | server | `10000` | Cap on distinct assets (0 = unlimited) |
| `STATS_INTERVAL` | server | `10s` | How often ingest counters are logged |
| `ASSETS` | simulator | `10` | Number of simulated assets |
| `INTERVAL` | simulator | `1s` | Publish interval |
| `NOISE_SCALE` | simulator | `1` | Multiplier on sensor noise (0 = clean sine) |
| `PUBLISH_CONCURRENCY` | simulator | `32` | Max in-flight publishes |

## Topics & payload

| Topic | Payload | Notes |
|---|---|---|
| `twin/sensors/{asset_id}/{metric}` | `{"value": 1503.2, "timestamp": "2026-10-11T12:00:00Z"}` | Asset and metric come from the topic only |
| `twin/status/{client_id}` | `online` / `offline` | Retained; `offline` is also the last-will |

## Design decisions worth knowing

- **Stale readings are dropped.** Workers don't preserve order and a resumed MQTT session replays
  queued messages, so the store only accepts a reading if it is not older than the one it holds.
- **Backpressure, not loss.** When the queue is full the MQTT receive path blocks. Swap
  `pool.Submit` for a non-blocking send + drop counter if you prefer freshness over completeness.
- **Ordered shutdown.** stop receiving (disconnect) → drain queue → exit.
- **Core packages don't import MQTT.** `state`, `pool`, `ingest`, `sim`, `topics`, `config` are plain
  Go and unit-tested; only `mqttx` and `cmd/*` touch paho.

## Development

```bash
make check    # vet + tests with the race detector (what CI runs)
make bench    # store benchmarks
```

## Roadmap

- [x] **0 Setup** – module, Compose + Mosquitto, `slog`
- [x] **1 Simulator** – N assets, configurable interval/noise, topic scheme above
- [x] **2 Ingestion** – wildcard subscription, bounded worker pool, graceful shutdown _(50–100 asset load test against a real broker: still to do)_
- [x] **3 State store** – `RWMutex`, race tests + benchmarks
- [ ] **4 Broadcast hub / WebSocket** – snapshot on connect, then deltas
- [ ] **5 Anomaly hook** – `AnomalyDetector` interface, threshold implementation sets `Status`
- [ ] **6 Polish** – `/health`, `/metrics`, integration test (simulator + server in-process)
- [ ] **7 Dashboard** – single HTML page over the WebSocket
