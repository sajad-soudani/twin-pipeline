// Package config reads settings from environment variables, with defaults.
// The environment lookup is injected so tests don't touch the real one.
package config

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Getenv matches os.Getenv.
type Getenv func(string) string

type Common struct {
	Env       string
	BrokerURL string
	ClientID  string
}

func (c Common) Production() bool { return c.Env == "production" }

type Server struct {
	Common
	Workers       int
	QueueSize     int
	MaxAssets     int
	StatsInterval time.Duration
}

type Simulator struct {
	Common
	Assets      int
	Interval    time.Duration
	NoiseScale  float64
	Concurrency int // max in-flight publishes per tick
}

func LoadServer(get Getenv) (Server, error) {
	l := loader{get: get}
	c := Server{
		Common:        l.common("twin-server"),
		Workers:       l.integer("WORKERS", 10, 1),
		QueueSize:     l.integer("QUEUE_SIZE", 1000, 1),
		MaxAssets:     l.integer("MAX_ASSETS", 10000, 0),
		StatsInterval: l.duration("STATS_INTERVAL", 10*time.Second),
	}
	return c, l.err()
}

func LoadSimulator(get Getenv) (Simulator, error) {
	l := loader{get: get}
	c := Simulator{
		Common:      l.common("simulator"),
		Assets:      l.integer("ASSETS", 10, 1),
		Interval:    l.duration("INTERVAL", time.Second),
		NoiseScale:  l.float("NOISE_SCALE", 1),
		Concurrency: l.integer("PUBLISH_CONCURRENCY", 32, 1),
	}
	return c, l.err()
}

type loader struct {
	get  Getenv
	errs []error
}

func (l *loader) err() error { return errors.Join(l.errs...) }

func (l *loader) str(key, def string) string {
	if v := l.get(key); v != "" {
		return v
	}
	return def
}

func (l *loader) common(defaultClientID string) Common {
	return Common{
		Env:       l.str("ENV", "development"),
		BrokerURL: l.str("MQTT_BROKER_URL", "mqtt://127.0.0.1:1883"),
		ClientID:  l.str("MQTT_CLIENT_ID", defaultClientID),
	}
}

func (l *loader) integer(key string, def, min int) int {
	v := l.get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min {
		l.errs = append(l.errs, fmt.Errorf("%s=%q: want an integer >= %d", key, v, min))
		return def
	}
	return n
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	v := l.get(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		l.errs = append(l.errs, fmt.Errorf("%s=%q: want a positive duration like 500ms or 2s", key, v))
		return def
	}
	return d
}

func (l *loader) float(key string, def float64) float64 {
	v := l.get(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 {
		l.errs = append(l.errs, fmt.Errorf("%s=%q: want a number >= 0", key, v))
		return def
	}
	return f
}
