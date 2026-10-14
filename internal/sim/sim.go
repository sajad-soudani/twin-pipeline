// Package sim generates realistic-looking sensor values. It has no MQTT
// dependency, so it is deterministic and unit-testable with a seeded RNG.
package sim

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/sajad-soudani/twin-pipeline/internal/model"
)

// Profile describes one sensor: baseline + sine wave + gaussian noise.
// (Alert thresholds deliberately do NOT live here: they are the anomaly
// detector's business, not the sensor's.)
type Profile struct {
	Baseline  float64
	Amplitude float64
	Period    time.Duration
	NoiseStd  float64
}

// DieselGenerator is keyed by metric name, not by topic.
var DieselGenerator = map[string]Profile{
	model.MetricRPM:            {Baseline: 1500, Amplitude: 15, Period: 2 * time.Minute, NoiseStd: 3},
	model.MetricCoolantTemp:    {Baseline: 88, Amplitude: 5, Period: 10 * time.Minute, NoiseStd: 0.8},
	model.MetricOilPressure:    {Baseline: 45, Amplitude: 5, Period: 8 * time.Minute, NoiseStd: 1.2},
	model.MetricOilTemp:        {Baseline: 100, Amplitude: 6, Period: 12 * time.Minute, NoiseStd: 1.0},
	model.MetricExhaustTemp:    {Baseline: 480, Amplitude: 60, Period: 5 * time.Minute, NoiseStd: 8},
	model.MetricBatteryVoltage: {Baseline: 13.6, Amplitude: 0.3, Period: 15 * time.Minute, NoiseStd: 0.05},
	model.MetricVibration:      {Baseline: 1.2, Amplitude: 0.3, Period: 3 * time.Minute, NoiseStd: 0.15},
}

// Generator is NOT safe for concurrent use (it owns a *rand.Rand); sample it
// from one goroutine and hand the values to publishers.
type Generator struct {
	p          Profile
	noiseScale float64
	phase      float64
	rng        *rand.Rand
}

// NewGenerator: phase (radians) lets assets differ; noiseScale multiplies
// NoiseStd (0 = clean sine, handy in tests).
func NewGenerator(p Profile, noiseScale, phase float64, rng *rand.Rand) *Generator {
	return &Generator{p: p, noiseScale: noiseScale, phase: phase, rng: rng}
}

// At returns the value `elapsed` after the generator started.
func (g *Generator) At(elapsed time.Duration) float64 {
	angle := 2*math.Pi*elapsed.Seconds()/g.p.Period.Seconds() + g.phase
	wave := g.p.Amplitude * math.Sin(angle)
	noise := g.rng.NormFloat64() * g.p.NoiseStd * g.noiseScale
	return g.p.Baseline + wave + noise
}

// Source is one (asset, metric) stream.
type Source struct {
	AssetID string
	Metric  string
	Gen     *Generator
}

// NewFleet builds assets × metrics sources in a stable order. Each asset gets
// its own phase so 100 assets don't all draw the same curve.
func NewFleet(assets int, noiseScale float64, rng *rand.Rand) []Source {
	metrics := make([]string, 0, len(DieselGenerator))
	for m := range DieselGenerator {
		metrics = append(metrics, m)
	}
	slices.Sort(metrics)

	out := make([]Source, 0, assets*len(metrics))
	for i := range assets {
		id := fmt.Sprintf("engine-%03d", i+1)
		phase := rng.Float64() * 2 * math.Pi
		for _, m := range metrics {
			out = append(out, Source{
				AssetID: id,
				Metric:  m,
				Gen:     NewGenerator(DieselGenerator[m], noiseScale, phase, rng),
			})
		}
	}
	return out
}
