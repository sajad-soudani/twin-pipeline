package ingestion

import (
	"log/slog"

	"github.com/eclipse/paho.golang/paho"
	"github.com/sajad-soudani/twin-pipeline/internal/state"
	"github.com/sajad-soudani/twin-pipeline/pkg/models"
)

func Engine(workerID int, payload []byte, qos byte, retain bool, properties paho.UserProperties, store *state.Store) {

	slog.Info(
		"OnPublishRecv",
		"workerID", workerID,
		"qos", qos,
		"retain", retain,
		"payload", payload,
		"properties", properties,
	)

	var sd models.SensorData
	if err := models.UnMarshal(payload, &sd); err != nil {
		slog.Error("UnMarshal error occured", "error", err.Error())
	}

	store.Update(sd)

	slog.Info("sth", "payload", payload)
}
