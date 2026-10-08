package models

import (
	"encoding/json"
	"time"
)

type SensorData struct {
	AssetID   string    `json:"asset_id"`
	Metric    string    `json:"metric"`
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

type AssetState struct {
	AssetID    string             `json:"asset_id"`
	Metrics    map[string]float64 `json:"metrics"`
	LastUpdate time.Time          `json:"last_update"`
	Status     string             `json:"status"`
}

func Marshal(assetID, metric string, value float64) ([]byte, error) {
	sd := SensorData{
		AssetID:   assetID,
		Metric:    metric,
		Value:     value,
		Timestamp: time.Now(),
	}
	return json.Marshal(sd)
}

func UnMarshal(rawJson []byte, sd *SensorData) error {
	return json.Unmarshal(rawJson, sd)
}
