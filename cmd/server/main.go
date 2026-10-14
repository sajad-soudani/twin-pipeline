package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/sajad-soudani/twin-pipeline/internal/config"
	"github.com/sajad-soudani/twin-pipeline/internal/ingest"
	"github.com/sajad-soudani/twin-pipeline/internal/logging"
	"github.com/sajad-soudani/twin-pipeline/internal/mqttx"
	"github.com/sajad-soudani/twin-pipeline/internal/pool"
	"github.com/sajad-soudani/twin-pipeline/internal/state"
	"github.com/sajad-soudani/twin-pipeline/internal/topics"
)

func main() {
	cfg, err := config.LoadServer(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	logging.Setup(cfg.Production())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// run returns instead of panicking, so deferred cleanup always executes.
	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Server) error {
	store := state.NewStore(cfg.MaxAssets)
	handler := ingest.NewHandler(store)
	workers := pool.New(cfg.Workers, cfg.QueueSize, handler.Handle)

	// Incoming messages must keep flowing until we have DISCONNECTED, so this
	// context is separate from the signal context and cancelled last.
	ingestCtx, stopIngest := context.WithCancel(context.Background())
	defer stopIngest()

	fatal := make(chan error, 1)

	mqttCfg, err := mqttx.NewConfig(mqttx.Options{
		BrokerURL:  cfg.BrokerURL,
		ClientID:   cfg.ClientID,
		CleanStart: false, // keep the session: QoS 1 messages queue while we restart

		OnUp: subscribe,
		OnFatal: func(err error) {
			select {
			case fatal <- err:
			default:
			}
		},
		OnPublish: func(pr paho.PublishReceived) (bool, error) {
			msg := ingest.Message{Topic: pr.Packet.Topic, Payload: pr.Packet.Payload}
			if err := workers.Submit(ingestCtx, msg); err != nil {
				slog.Warn("message dropped", "topic", msg.Topic, "err", err)
			}
			return true, nil
		},
	})
	if err != nil {
		return err
	}

	// The connection has its own lifetime so we can still publish "offline"
	// and disconnect cleanly AFTER the signal context is cancelled.
	connCtx, cancelConn := context.WithCancel(context.Background())
	defer cancelConn()

	cm, err := autopaho.NewConnection(connCtx, mqttCfg)
	if err != nil {
		return fmt.Errorf("create connection: %w", err)
	}
	if err := cm.AwaitConnection(ctx); err != nil {
		return fmt.Errorf("await connection: %w", err) // Ctrl-C while waiting lands here: clean exit
	}

	go logStats(ctx, cfg.StatsInterval, handler)

	var runErr error
	select {
	case <-ctx.Done():
		slog.Info("shutting down")
	case runErr = <-fatal:
	}

	// Shutdown order matters: stop the source, then drain, then exit.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mqttx.PublishStatus(shutdownCtx, cm, cfg.ClientID, mqttx.Offline)
	if err := cm.Disconnect(shutdownCtx); err != nil {
		slog.Warn("disconnect", "err", err)
	}
	select {
	case <-cm.Done():
	case <-shutdownCtx.Done():
		slog.Warn("timed out waiting for mqtt shutdown")
	}

	stopIngest()    // release any Submit still blocked on a full queue
	workers.Close() // no more input...
	workers.Wait()  // ...let workers finish what is queued

	slog.Info("stopped", "stats", handler.Stats())
	return runErr
}

// subscribe fails loudly: a server that is connected but subscribed to
// nothing looks healthy and does nothing, the worst kind of bug.
func subscribe(cm *autopaho.ConnectionManager, _ *paho.Connack) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ack, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: topics.SensorFilter, QoS: 1}},
	})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	for i, rc := range ack.Reasons {
		if rc >= 0x80 { // MQTT 5: reason codes >= 0x80 mean failure
			return fmt.Errorf("subscription %d refused (reason code 0x%02x)", i, rc)
		}
	}
	return nil
}

func logStats(ctx context.Context, every time.Duration, h *ingest.Handler) {
	t := time.NewTicker(every)
	defer t.Stop()

	var last ingest.Stats
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s := h.Stats()
			slog.Info("ingest",
				"accepted_per_s", float64(s.Accepted-last.Accepted)/every.Seconds(),
				"stale", s.Stale, "invalid", s.Invalid, "rejected", s.Rejected)
			last = s
		}
	}
}
