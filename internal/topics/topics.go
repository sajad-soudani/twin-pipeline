// Package topics is the single place that knows the MQTT topic layout:
//
//	twin/sensors/{asset_id}/{metric}   readings
//	twin/status/{client_id}            retained presence, one per client
package topics

import (
	"fmt"
	"strings"
)

const (
	sensorPrefix = "twin/sensors/"
	statusPrefix = "twin/status/"

	// SensorFilter is the subscription covering every asset and metric.
	// Filtering is done by the broker, so the server needs no wildcard matching.
	SensorFilter = "twin/sensors/+/+"

	maxSegmentLen = 64
)

func Sensor(assetID, metric string) string { return sensorPrefix + assetID + "/" + metric }

func Status(clientID string) string { return statusPrefix + clientID }

// ParseSensor extracts and validates asset and metric from a sensor topic.
func ParseSensor(topic string) (assetID, metric string, err error) {
	rest, ok := strings.CutPrefix(topic, sensorPrefix)
	if !ok {
		return "", "", fmt.Errorf("topic %q is not a sensor topic", topic)
	}
	assetID, metric, ok = strings.Cut(rest, "/")
	if !ok || !validSegment(assetID) || !validSegment(metric) {
		return "", "", fmt.Errorf("topic %q does not match %s{asset}/{metric}", topic, sensorPrefix)
	}
	return assetID, metric, nil
}

// validSegment keeps keys bounded and boring: anyone who can publish can
// otherwise invent arbitrary strings that become map keys.
func validSegment(s string) bool {
	if s == "" || len(s) > maxSegmentLen {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return false
		}
	}
	return true
}
