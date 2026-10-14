// Package mqttx holds the MQTT boilerplate both binaries shared by copy-paste:
// connection config, per-client retained presence, and the last-will.
package mqttx

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/sajad-soudani/twin-pipeline/internal/topics"
)

const (
	Online  = "online"
	Offline = "offline"
)

type Options struct {
	BrokerURL  string
	ClientID   string
	CleanStart bool

	// OnUp runs after every (re)connect, e.g. to subscribe. If it returns an
	// error the connection is considered unusable and OnFatal is called.
	OnUp func(cm *autopaho.ConnectionManager, ack *paho.Connack) error
	// OnFatal is how a binary learns it should shut down.
	OnFatal func(err error)
	// OnPublish receives incoming messages (server only).
	OnPublish func(pr paho.PublishReceived) (bool, error)
}

func NewConfig(o Options) (autopaho.ClientConfig, error) {
	u, err := url.Parse(o.BrokerURL)
	if err != nil {
		return autopaho.ClientConfig{}, fmt.Errorf("parse broker url %q: %w", o.BrokerURL, err)
	}

	willDelay := uint32(5) // ride out brief reconnects before announcing "offline"

	cfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{u},
		KeepAlive:  20,

		CleanStartOnInitialConnection: o.CleanStart,
		SessionExpiryInterval:         60,

		// Presence is per client. The original shared one retained topic
		// (twin/status) between simulator and server, so they overwrote each other.
		WillMessage: &paho.WillMessage{
			Topic:   topics.Status(o.ClientID),
			Payload: []byte(Offline),
			QoS:     1,
			Retain:  true,
		},
		WillProperties: &paho.WillProperties{WillDelayInterval: &willDelay},

		OnConnectionUp: func(cm *autopaho.ConnectionManager, ack *paho.Connack) {
			slog.Info("mqtt connection up", "client", o.ClientID, "session_present", ack.SessionPresent)
			if o.OnUp != nil {
				if err := o.OnUp(cm, ack); err != nil {
					slog.Error("mqtt setup failed", "err", err)
					if o.OnFatal != nil {
						o.OnFatal(err)
					}
					return
				}
			}
			// Own goroutine: don't block autopaho's callback on a network round trip.
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				PublishStatus(ctx, cm, o.ClientID, Online)
			}()
		},
		OnConnectError: func(err error) { slog.Error("mqtt connect failed", "err", err) },

		ClientConfig: paho.ClientConfig{
			ClientID:      o.ClientID,
			OnClientError: func(err error) { slog.Error("mqtt client error", "err", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				slog.Error("mqtt server disconnect", "reason_code", d.ReasonCode)
			},
		},
	}
	if o.OnPublish != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){o.OnPublish}
	}
	return cfg, nil
}

// PublishStatus publishes retained presence for clientID.
func PublishStatus(ctx context.Context, cm *autopaho.ConnectionManager, clientID, status string) {
	_, err := cm.Publish(ctx, &paho.Publish{
		Topic:   topics.Status(clientID),
		QoS:     1,
		Retain:  true,
		Payload: []byte(status),
	})
	if err != nil {
		slog.Error("publish status failed", "status", status, "err", err)
	}
}
