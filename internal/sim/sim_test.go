package sim

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

func newRNG() *rand.Rand { return rand.New(rand.NewPCG(1, 2)) }

func TestCleanSineWave(t *testing.T) {
	p := Profile{Baseline: 100, Amplitude: 10, Period: 4 * time.Second, NoiseStd: 5}
	g := NewGenerator(p, 0, 0, newRNG()) // noiseScale 0 => deterministic

	cases := []struct {
		at   time.Duration
		want float64
	}{
		{0, 100}, {time.Second, 110}, {2 * time.Second, 100}, {3 * time.Second, 90},
	}
	for _, c := range cases {
		if got := g.At(c.at); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("At(%v) = %v, want %v", c.at, got, c.want)
		}
	}
}

func TestSameSeedSameOutput(t *testing.T) {
	p := DieselGenerator["rpm"]
	a := NewGenerator(p, 1, 0, newRNG())
	b := NewGenerator(p, 1, 0, newRNG())
	for i := range 20 {
		d := time.Duration(i) * time.Second
		if a.At(d) != b.At(d) {
			t.Fatal("seeded generators diverged")
		}
	}
}

func TestProfilesAreSane(t *testing.T) {
	for name, p := range DieselGenerator {
		if p.Period <= 0 {
			t.Errorf("%s: period must be > 0 (would divide by zero)", name)
		}
	}
}

// Regression: the original's "N assets" loop published every sample under the
// same asset ID. A fleet must contain N DISTINCT assets.
func TestFleetHasDistinctAssets(t *testing.T) {
	fleet := NewFleet(50, 1, newRNG())
	if want := 50 * len(DieselGenerator); len(fleet) != want {
		t.Fatalf("got %d sources, want %d", len(fleet), want)
	}
	assets := map[string]bool{}
	for _, s := range fleet {
		assets[s.AssetID] = true
	}
	if len(assets) != 50 {
		t.Fatalf("got %d distinct assets, want 50", len(assets))
	}
}
