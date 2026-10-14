// Package ingest turns raw MQTT messages into store updates.
package ingest

import (
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/sajad-soudani/twin-pipeline/internal/model"
	"github.com/sajad-soudani/twin-pipeline/internal/state"
	"github.com/sajad-soudani/twin-pipeline/internal/topics"
)

// Message is the transport-agnostic input: only what ingestion needs.
type Message struct {
	Topic   string
	Payload []byte
}

// Stats are cumulative counters. Per-message INFO logs don't scale to
// thousands of messages per second; counters do.
type Stats struct {
	Accepted uint64
	Stale    uint64
	Invalid  uint64
	Rejected uint64 // refused by the store (asset limit)
}

type Handler struct {
	store                              *state.Store
	accepted, stale, invalid, rejected atomic.Uint64
}

func NewHandler(store *state.Store) *Handler { return &Handler{store: store} }

// Handle is safe for concurrent use.
func (h *Handler) Handle(workerID int, m Message) {
	assetID, metric, err := topics.ParseSensor(m.Topic)
	if err != nil {
		h.invalid.Add(1)
		slog.Debug("bad topic", "worker", workerID, "err", err)
		return
	}

	r, err := model.DecodeReading(m.Payload)
	if err != nil {
		h.invalid.Add(1) // and crucially: return, so nothing reaches the store
		slog.Debug("bad payload", "worker", workerID, "topic", m.Topic, "err", err)
		return
	}

	applied, err := h.store.Update(assetID, metric, r)
	switch {
	case errors.Is(err, state.ErrTooManyAssets):
		h.rejected.Add(1)
		slog.Warn("asset rejected", "asset", assetID, "err", err)
	case err != nil:
		h.invalid.Add(1)
		slog.Error("store update failed", "asset", assetID, "err", err)
	case !applied:
		h.stale.Add(1)
	default:
		h.accepted.Add(1)
		slog.Debug("reading", "worker", workerID, "asset", assetID, "metric", metric, "value", r.Value)
	}
}

func (h *Handler) Stats() Stats {
	return Stats{
		Accepted: h.accepted.Load(),
		Stale:    h.stale.Load(),
		Invalid:  h.invalid.Load(),
		Rejected: h.rejected.Load(),
	}
}
