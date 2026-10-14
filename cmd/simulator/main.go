package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/sajad-soudani/twin-pipeline/internal/config"
	"github.com/sajad-soudani/twin-pipeline/internal/logging"
	"github.com/sajad-soudani/twin-pipeline/internal/model"
	"github.com/sajad-soudani/twin-pipeline/internal/mqttx"
	"github.com/sajad-soudani/twin-pipeline/internal/sim"
	"github.com/sajad-soudani/twin-pipeline/internal/topics"
)

const messageExpirySeconds = 30

func main() {
	cfg, err := config.LoadSimulator(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	logging.Setup(cfg.Production())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("simulator failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Simulator) error {
	mqttCfg, err := mqttx.NewConfig(mqttx.Options{
		BrokerURL:  cfg.BrokerURL,
		ClientID:   cfg.ClientID,
		CleanStart: true, // a simulator has no session worth keeping
	})
	if err != nil {
		return err
	}

	connCtx, cancelConn := context.WithCancel(context.Background())
	defer cancelConn()

	cm, err := autopaho.NewConnection(connCtx, mqttCfg)
	if err != nil {
		return fmt.Errorf("create connection: %w", err)
	}
	if err := cm.AwaitConnection(ctx); err != nil { // ctx, not connCtx: Ctrl-C must work here
		return fmt.Errorf("await connection: %w", err)
	}

	// Seed from the clock; inject a fixed seed here for reproducible runs.
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))
	sources := sim.NewFleet(cfg.Assets, cfg.NoiseScale, rng)
	slog.Info("simulating", "assets", cfg.Assets, "streams", len(sources), "interval", cfg.Interval)

	start := time.Now()
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case now := <-ticker.C:
			publishTick(ctx, cm, sources, now, now.Sub(start), cfg.Concurrency)
		}
	}

	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mqttx.PublishStatus(shutdownCtx, cm, cfg.ClientID, mqttx.Offline)
	if err := cm.Disconnect(shutdownCtx); err != nil {
		slog.Warn("disconnect", "err", err)
	}
	select {
	case <-cm.Done():
	case <-shutdownCtx.Done():
	}
	return nil
}

type outgoing struct {
	topic   string
	payload []byte
}

// publishTick samples every source (single goroutine: generators aren't
// thread-safe), then publishes with bounded concurrency and WAITS for the
// batch. The original spawned one goroutine per message with no bound and no
// wait, so a slow broker meant an ever-growing pile of goroutines.
func publishTick(ctx context.Context, cm *autopaho.ConnectionManager, sources []sim.Source, now time.Time, elapsed time.Duration, concurrency int) {
	batch := make([]outgoing, 0, len(sources))
	for _, s := range sources {
		payload, err := model.EncodeReading(s.Gen.At(elapsed), now)
		if err != nil {
			slog.Error("encode reading", "asset", s.AssetID, "metric", s.Metric, "err", err)
			continue // never publish a nil payload
		}
		batch = append(batch, outgoing{topics.Sensor(s.AssetID, s.Metric), payload})
	}

	var (
		wg     sync.WaitGroup
		failed atomic.Int64
		sem    = make(chan struct{}, concurrency)
	)
	for _, m := range batch {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			if err := publish(ctx, cm, m); err != nil && ctx.Err() == nil {
				failed.Add(1)
			}
		}()
	}
	wg.Wait()

	// One line per tick, not one per failed message (that is 70+ lines/s in an outage).
	if n := failed.Load(); n > 0 {
		slog.Warn("publish failures", "failed", n, "of", len(batch))
	}
}

func publish(ctx context.Context, cm *autopaho.ConnectionManager, m outgoing) error {
	expiry := uint32(messageExpirySeconds)
	_, err := cm.Publish(ctx, &paho.Publish{
		Topic:   m.topic,
		QoS:     1,
		Payload: m.payload,
		Properties: &paho.PublishProperties{
			MessageExpiry: &expiry,
			ContentType:   "application/json",
		},
	})
	return err
}
