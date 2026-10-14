package model

// Metric names used on the wire (last topic segment). Defined once so the
// simulator and the future anomaly detector can't drift apart.
const (
	MetricRPM            = "rpm"
	MetricCoolantTemp    = "coolant-temp"
	MetricOilPressure    = "oil-pressure"
	MetricOilTemp        = "oil-temp"
	MetricExhaustTemp    = "exhaust-temp"
	MetricBatteryVoltage = "battery-voltage"
	MetricVibration      = "vibration"
)
