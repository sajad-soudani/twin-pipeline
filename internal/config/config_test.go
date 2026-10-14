package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) Getenv { return func(k string) string { return m[k] } }

func TestServerDefaults(t *testing.T) {
	c, err := LoadServer(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.Workers != 10 || c.QueueSize != 1000 || c.BrokerURL != "mqtt://127.0.0.1:1883" || c.ClientID != "twin-server" || c.Production() {
		t.Fatalf("%+v", c)
	}
}

func TestSimulatorOverrides(t *testing.T) {
	c, err := LoadSimulator(env(map[string]string{
		"ASSETS": "100", "INTERVAL": "250ms", "NOISE_SCALE": "2.5", "ENV": "production",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Assets != 100 || c.Interval != 250*time.Millisecond || c.NoiseScale != 2.5 || !c.Production() {
		t.Fatalf("%+v", c)
	}
}

func TestInvalidValuesAreAllReported(t *testing.T) {
	_, err := LoadServer(env(map[string]string{"WORKERS": "0", "QUEUE_SIZE": "abc", "STATS_INTERVAL": "-1s"}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, key := range []string{"WORKERS", "QUEUE_SIZE", "STATS_INTERVAL"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error should mention %s: %v", key, err)
		}
	}
}
