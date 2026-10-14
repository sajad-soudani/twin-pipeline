// Package model holds the domain types shared by the simulator and the server.
package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
)

// Status is the health of an asset. The anomaly detector (roadmap phase 5)
// will be the only writer.
type Status string

const (
	StatusNominal  Status = "nominal"
	StatusWarning  Status = "warning"
	StatusCritical Status = "critical"
)

// ErrInvalidReading is wrapped by every validation failure so callers can
// use errors.Is instead of matching strings.
var ErrInvalidReading = errors.New("invalid reading")

// Reading is one sample of one metric. It is also the MQTT payload.
// The asset and metric are NOT in the payload: they live in the topic,
// so they can never disagree with it.
type Reading struct {
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// Validate rejects readings the store must never see.
func (r Reading) Validate() error {
	switch {
	case math.IsNaN(r.Value) || math.IsInf(r.Value, 0):
		return fmt.Errorf("%w: value is not finite", ErrInvalidReading)
	case r.Timestamp.IsZero():
		return fmt.Errorf("%w: missing timestamp", ErrInvalidReading)
	}
	return nil
}

// EncodeReading builds the wire payload. The timestamp is passed in (rather
// than calling time.Now here) so callers and tests control the clock.
func EncodeReading(value float64, ts time.Time) ([]byte, error) {
	return json.Marshal(Reading{Value: value, Timestamp: ts.UTC()})
}

// DecodeReading parses AND validates a payload.
func DecodeReading(payload []byte) (Reading, error) {
	var r Reading
	if err := json.Unmarshal(payload, &r); err != nil {
		return Reading{}, fmt.Errorf("%w: %v", ErrInvalidReading, err)
	}
	if err := r.Validate(); err != nil {
		return Reading{}, err
	}
	return r, nil
}

// AssetState is the latest known state of one asset.
type AssetState struct {
	AssetID    string             `json:"asset_id"`
	Metrics    map[string]Reading `json:"metrics"`
	LastUpdate time.Time          `json:"last_update"`
	Status     Status             `json:"status"`
}
