package ingestion

import (
	"log/slog"

	"github.com/eclipse/paho.golang/paho"
)

func Engine(pr paho.PublishReceived) (bool, error) {
	p := pr.Packet
	slog.Info(
		"OnPublishRecv",
		"topic", p.Topic,
		"qos", p.QoS,
		"retain", p.Retain,
		"payload", p.Payload,
		"properties", p.Properties.User,
	)

	return true, nil
}
