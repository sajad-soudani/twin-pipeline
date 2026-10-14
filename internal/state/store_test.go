package state

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sajad-soudani/twin-pipeline/internal/model"
)

var t0 = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

func reading(v float64, offset time.Duration) model.Reading {
	return model.Reading{Value: v, Timestamp: t0.Add(offset)}
}

func TestUpdateAndGet(t *testing.T) {
	s := NewStore(0)
	if applied, err := s.Update("a", "rpm", reading(1500, 0)); !applied || err != nil {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	got, ok := s.Get("a")
	if !ok || got.Metrics["rpm"].Value != 1500 || got.Status != model.StatusNominal {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
	if _, ok := s.Get("missing"); ok {
		t.Fatal("unexpected asset")
	}
}

// Regression for the original design: concurrent workers can deliver readings
// out of order, and last-write-wins would let stale data overwrite fresh data.
func TestStaleReadingIsIgnored(t *testing.T) {
	s := NewStore(0)
	s.Update("a", "rpm", reading(2, 2*time.Second))

	applied, err := s.Update("a", "rpm", reading(1, 1*time.Second)) // older
	if applied || err != nil {
		t.Fatalf("stale reading: applied=%v err=%v", applied, err)
	}
	got, _ := s.Get("a")
	if got.Metrics["rpm"].Value != 2 || !got.LastUpdate.Equal(t0.Add(2*time.Second)) {
		t.Fatalf("stale write leaked: %+v", got)
	}
}

func TestLastUpdateNeverGoesBackwards(t *testing.T) {
	s := NewStore(0)
	s.Update("a", "rpm", reading(1, 10*time.Second))
	s.Update("a", "oil-temp", reading(2, 5*time.Second)) // different metric, older
	got, _ := s.Get("a")
	if !got.LastUpdate.Equal(t0.Add(10 * time.Second)) {
		t.Fatalf("LastUpdate = %v", got.LastUpdate)
	}
}

func TestAssetLimit(t *testing.T) {
	s := NewStore(2)
	s.Update("a", "m", reading(1, 0))
	s.Update("b", "m", reading(1, 0))
	if _, err := s.Update("c", "m", reading(1, 0)); !errors.Is(err, ErrTooManyAssets) {
		t.Fatalf("want ErrTooManyAssets, got %v", err)
	}
	if applied, err := s.Update("a", "m", reading(2, time.Second)); !applied || err != nil {
		t.Fatalf("existing asset must still update: %v %v", applied, err)
	}
}

func TestReturnedCopiesAreIsolated(t *testing.T) {
	s := NewStore(0)
	s.Update("a", "rpm", reading(1, 0))

	got, _ := s.Get("a")
	got.Metrics["rpm"] = reading(999, 0)
	snap := s.Snapshot()
	snap[0].Metrics["rpm"] = reading(999, 0)

	again, _ := s.Get("a")
	if again.Metrics["rpm"].Value != 1 {
		t.Fatal("caller mutated the store through a returned copy")
	}
}

func TestSnapshotSorted(t *testing.T) {
	s := NewStore(0)
	for _, id := range []string{"c", "a", "b"} {
		s.Update(id, "m", reading(1, 0))
	}
	snap := s.Snapshot()
	if len(snap) != 3 || snap[0].AssetID != "a" || snap[1].AssetID != "b" || snap[2].AssetID != "c" {
		t.Fatalf("not sorted: %+v", snap)
	}
}

// Run with -race: this is what proves the locking.
func TestConcurrentReadersAndWriters(t *testing.T) {
	s := NewStore(0)
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := range 500 {
				s.Update(fmt.Sprintf("asset-%d", w%3), "rpm", reading(float64(i), time.Duration(i)*time.Millisecond))
			}
		}()
		go func() {
			defer wg.Done()
			for range 500 {
				s.Snapshot()
				s.Get("asset-0")
			}
		}()
	}
	wg.Wait()

	for _, a := range s.Snapshot() {
		if got := a.Metrics["rpm"].Value; got != 499 {
			t.Errorf("%s: final rpm = %v, want 499 (newest reading must win)", a.AssetID, got)
		}
	}
}

func BenchmarkUpdateParallel(b *testing.B) {
	s := NewStore(0)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			s.Update(fmt.Sprintf("asset-%d", i%100), "rpm", reading(float64(i), time.Duration(i)))
		}
	})
}

func BenchmarkMixedReadWrite(b *testing.B) {
	s := NewStore(0)
	for i := range 100 {
		s.Update(fmt.Sprintf("asset-%d", i), "rpm", reading(1, 0))
	}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			if i%10 == 0 {
				s.Update("asset-1", "rpm", reading(float64(i), time.Duration(i)))
			} else {
				s.Get("asset-1")
			}
		}
	})
}
