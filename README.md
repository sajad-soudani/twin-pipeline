# twin-pipeline

## 5. Build Plan — Phased Milestones

### 📍 Phase 0 — Setup
- [x] `go mod init github.com/sajad-soudani/twin-pipeline`
- [x] Set up repo structure (see §6)
- [x] Docker Compose with a Mosquitto broker
- [x] Basic `slog` logger wired up
- [x] Commit: "project scaffold"

### 📍 Phase 1 — Sensor Simulator
- [x] Write a simulator that spins up N virtual "assets," each publishing readings on an interval with realistic noise (e.g. sine wave + random jitter for temperature)
- [x] Publish readings to MQTT topic `sensors/{asset_id}/{metric}`
- [x] Make interval, asset count, and noise level configurable
- [x] **Milestone check:** you can watch readings being generated in the logs

### 📍 Phase 2 — Ingestion Layer
- [x] Subscribe to all sensor topics (wildcard subscription)
- [x] Each incoming message is handed to a worker pool (bounded number of goroutines) for validation/parsing
- [x] Use a `context.Context` for graceful shutdown
- [x] **Milestone check:** readings are being received and parsed without dropped messages under load (test with 50–100 simulated assets)

### 📍 Phase 3 — Concurrent State Store
- [ ] Implement the state store with `sync.RWMutex`
- [ ] On each reading, update the asset's latest metric value and `LastUpdate`
- [ ] Write a benchmark (`go test -bench`) simulating concurrent reads/writes to prove there's no race condition
- [ ] **Milestone check:** `go test -race ./...` passes clean under concurrent load

### 📍 Phase 4 — Broadcast Hub / WebSocket Streaming
- [ ] Implement a pub/sub hub: clients connect via WebSocket, get the current full state snapshot, then receive deltas as they happen
- [ ] Handle client disconnects gracefully (don't leak goroutines)
- [ ] **Milestone check:** open the WS endpoint in `websocat` or a tiny HTML page and watch live updates stream in

### 📍 Phase 5 — Anomaly Hook
- [ ] Define an interface: `type AnomalyDetector interface { Check(AssetState) (isAnomaly bool, reason string) }`
- [ ] Implement a trivial threshold-based detector for now (e.g. temp > 80 = warning)
- [ ] Wire it into the state update path so `AssetState.Status` gets set
- [ ] **This is your seam for the AI project later**

### 📍 Phase 6 — Polish
- [ ] Add a `/health` and `/metrics` endpoint
- [ ] Dockerize the whole thing (`docker-compose up` should run simulator + ingestion + broker together)
- [ ] Write a README with an architecture diagram and a GIF/screenshot of the live dashboard
- [ ] Add basic integration test: spin up simulator + ingestion in-process, assert state store reflects readings within N seconds

### 📍 Phase 7 — Minimal Dashboard
- [ ] A single HTML/JS page (or tiny React app) that connects to the WebSocket and renders asset cards with live-updating values and color-coded status
