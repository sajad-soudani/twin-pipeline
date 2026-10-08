package main

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/sajad-soudani/twin-pipeline/internal"
	"github.com/sajad-soudani/twin-pipeline/internal/ingestion"
)

type Job struct {
	Topic   string
	QoS     byte
	Payload []byte
	Retain  bool
}

func StartWorkerPool(ctx context.Context, count int, jobs <-chan Job) {
	for i := range count {
		go worker(ctx, i, jobs)
	}
}

func worker(ctx context.Context, id int, jobs <-chan Job) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			slog.Info(
				"OnPublishRecv",
				"workerID", id,
				"topic", job.Topic,
				"qos", job.QoS,
				"retain", job.Retain,
				"payload", job.Payload,
			)
		}
	}
}

func main() {
	var handler slog.Handler
	if os.Getenv("ENV") == "production" {
		handler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	u, uErr := url.Parse("mqtt://127.0.0.1:1883")
	if uErr != nil {
		slog.Error("MQTT URL parsing error", "error", uErr.Error())
	}

	jobs := make(chan Job, 1000)
	StartWorkerPool(ctx, 10, jobs)

	willDelay := uint32(5)

	cfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{u},
		KeepAlive:  20,

		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         60,

		WillMessage: &paho.WillMessage{
			Topic:   internal.TopicStatus,
			Payload: []byte("offline"),
			QoS:     1,
			Retain:  true,
		},
		WillProperties: &paho.WillProperties{WillDelayInterval: &willDelay},

		OnConnectionUp: func(cm *autopaho.ConnectionManager, c *paho.Connack) {
			slog.Info("Connection Up", "isSessionPresent", c.SessionPresent)

			_, subErr := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: internal.TopicStatus, QoS: 0},
					{Topic: internal.TopicEngineAll, QoS: 1},
				},
			})

			if subErr != nil {
				slog.Error("OnConnectionUp sub failed", "error", subErr.Error())
				return
			}

			go ingestion.Status(context.Background(), cm, "online")
		},

		OnConnectError: func(err error) {
			slog.Error("OnConnectionError", "error", err.Error())
		},

		ClientConfig: paho.ClientConfig{
			ClientID: "twin-server",

			OnPublishReceived: []func(paho.PublishReceived) (bool, error){

				func(pr paho.PublishReceived) (bool, error) {
					jobs <- Job{
						Topic:   pr.Packet.Topic,
						QoS:     pr.Packet.QoS,
						Payload: pr.Packet.Payload,
						Retain:  pr.Packet.Retain,
					}

					if pr.Packet.Properties != nil {
						for _, up := range pr.Packet.Properties.User {
							slog.Info("user-properties", up.Key, up.Value)
						}
					}

					return true, nil
				},
				// ingestion.Engine,
			},

			OnClientError: func(err error) { slog.Error("OnClientError", "error", err.Error()) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				slog.Error("OnServerDisconnect", "error", d.ReasonCode)
			},
		},
	}

	cmCtx, cancelCmCtx := context.WithCancel(context.Background())

	cm, cmErr := autopaho.NewConnection(cmCtx, cfg)
	if cmErr != nil {
		slog.Error("Connection Manager Error", "error", cmErr.Error())
		panic(cmErr)
	}

	if err := cm.AwaitConnection(ctx); err != nil {
		panic(err)
	}

	if <-ctx.Done() == struct{}{} {
		slog.Warn("Shutting down...")
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second*5)

		ingestion.Status(shutdownCtx, cm, "offline")

		cancelShutdown()
		cancelCmCtx()
		stop()

	}

	<-cm.Done()

}
