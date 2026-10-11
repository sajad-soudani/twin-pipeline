package ingestion

import (
	"log/slog"

	"github.com/eclipse/paho.golang/paho"
)

func Engine(workerID int, topic string, payload []byte, qos byte, retain bool, properties paho.UserProperties) {
	slog.Info(
		"OnPublishRecv",
		"workerID", workerID,
		"topic", topic,
		"qos", qos,
		"retain", retain,
		"payload", payload,
		"properties", properties,
	)

}
