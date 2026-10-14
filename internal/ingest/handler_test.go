package ingest

import (
	"testing"
	"time"

	"github.com/sajad-soudani/twin-pipeline/internal/model"
	"github.com/sajad-soudani/twin-pipeline/internal/state"
	"github.com/sajad-soudani/twin-pipeline/internal/topics"
)

func payload(t *testing.T, v float64, ts time.Time) []byte {
	t.Helper()
	b, err := model.EncodeReading(v, ts)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestValidMessageReachesStore(t *testing.T) {
	store := state.NewStore(0)
	h := NewHandler(store)

	h.Handle(0, Message{topics.Sensor("engine-001", "rpm"), payload(t, 1500, time.Now())})

	got, ok := store.Get("engine-001")
	if !ok || got.Metrics["rpm"].Value != 1500 {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
	if s := h.Stats(); s.Accepted != 1 {
		t.Fatalf("stats %+v", s)
	}
}

// Regression: the original Engine() logged the unmarshal error but did not
// return, so garbage created an asset with an empty ID in the store.
func TestInvalidPayloadNeverTouchesStore(t *testing.T) {
	store := state.NewStore(0)
	h := NewHandler(store)

	h.Handle(0, Message{topics.Sensor("engine-001", "rpm"), []byte("not json")})
	h.Handle(0, Message{topics.Sensor("engine-001", "rpm"), []byte(`{"value":1}`)}) // no timestamp
	h.Handle(0, Message{"twin/sensors/bad topic/rpm", payload(t, 1, time.Now())})
	h.Handle(0, Message{"other/topic", payload(t, 1, time.Now())})

	if n := len(store.Snapshot()); n != 0 {
		t.Fatalf("store has %d assets, want 0: %+v", n, store.Snapshot())
	}
	if s := h.Stats(); s.Invalid != 4 || s.Accepted != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestStaleCounted(t *testing.T) {
	store := state.NewStore(0)
	h := NewHandler(store)
	now := time.Now()
	topic := topics.Sensor("a", "rpm")

	h.Handle(0, Message{topic, payload(t, 2, now)})
	h.Handle(0, Message{topic, payload(t, 1, now.Add(-time.Second))})

	if s := h.Stats(); s.Accepted != 1 || s.Stale != 1 {
		t.Fatalf("stats %+v", s)
	}
	if got, _ := store.Get("a"); got.Metrics["rpm"].Value != 2 {
		t.Fatalf("stale value overwrote fresh: %+v", got)
	}
}

func TestAssetLimitCounted(t *testing.T) {
	h := NewHandler(state.NewStore(1))
	h.Handle(0, Message{topics.Sensor("a", "m"), payload(t, 1, time.Now())})
	h.Handle(0, Message{topics.Sensor("b", "m"), payload(t, 1, time.Now())})
	if s := h.Stats(); s.Accepted != 1 || s.Rejected != 1 {
		t.Fatalf("stats %+v", s)
	}
}
