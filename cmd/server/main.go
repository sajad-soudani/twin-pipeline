package main

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

const (
	topicStatus            = "twin/status"
	topicData              = "twin/sensors"
	topicEngineTemperature = "twin/sensors/engine/temperature"
)

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
	defer stop()

	u, uErr := url.Parse("mqtt://127.0.0.1:1883")
	if uErr != nil {
		slog.Error("MQTT URL parsing error", "error", uErr.Error())
	}

	willDelay := uint32(5)

	cfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{u},
		KeepAlive:  20,

		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         60,

		WillMessage: &paho.WillMessage{
			Topic:   topicStatus,
			Payload: []byte("offline"),
			QoS:     1,
			Retain:  true,
		},
		WillProperties: &paho.WillProperties{WillDelayInterval: &willDelay},

		OnConnectionUp: func(cm *autopaho.ConnectionManager, c *paho.Connack) {
			slog.Info("Connection Up", "isSessionPresent", c.SessionPresent)

			_, subErr := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: topicStatus, QoS: 0},
					{Topic: topicData, QoS: 1},
					{Topic: topicEngineTemperature, QoS: 1},
				},
			})

			if subErr != nil {
				slog.Error("OnConnectionUp sub failed", "error", subErr.Error())
				return
			}

			go func() {
				_, pubErr := cm.Publish(context.Background(), &paho.Publish{
					Topic:   topicStatus,
					QoS:     1,
					Retain:  true,
					Payload: []byte("online"),
				})
				if pubErr != nil {
					slog.Error("OnConnectionUp pub online failed", "error", pubErr.Error())
				}
			}()
		},

		OnConnectError: func(err error) {
			slog.Error("OnConnectionError", "error", err.Error())
		},

		ClientConfig: paho.ClientConfig{
			ClientID: "twin-server",

			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					p := pr.Packet
					slog.Info(
						"OnPublishRecv",
						"topic", p.Topic,
						"qos", p.QoS,
						"retain", p.Retain,
						"payload", p.Payload,
					)

					if p.Properties != nil {
						for _, up := range p.Properties.User {
							slog.Info("user-properties", up.Key, up.Value)
						}
					}

					return true, nil
				},
			},

			OnClientError: func(err error) { slog.Error("OnClientError", "error", err.Error()) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				slog.Error("OnServerDisconnect", "error", d.ReasonCode)
			},
		},
	}

	cm, cmErr := autopaho.NewConnection(ctx, cfg)
	if cmErr != nil {
		slog.Error("Connection Manager Error", "error", cmErr.Error())
		panic(cmErr)
	}

	if err := cm.AwaitConnection(ctx); err != nil {
		panic(err)
	}

	<-cm.Done()

}
