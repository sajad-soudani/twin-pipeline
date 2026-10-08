package ingestion

import (
	"context"
	"log/slog"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/sajad-soudani/twin-pipeline/internal"
)

func Status(ctx context.Context, cm *autopaho.ConnectionManager, status string) {
	_, pubErr := cm.Publish(ctx, &paho.Publish{
		Topic:   internal.TopicStatus,
		QoS:     1,
		Retain:  true,
		Payload: []byte(status),
	})
	if pubErr != nil {
		slog.Error("pubish status failed", "error", pubErr.Error())
	}
}
