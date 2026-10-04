package internal

const (
	TopicStatus                   = "twin/status"
	TopicData                     = "twin/sensors"
	TopicEngine                   = "twin/sensors/engine/#"
	TopicEngineExhaustTemperature = TopicEngine + "/exhaust-temp"
	TopicEngineVibration          = TopicEngine + "/vibration"
	TopicEngineRPM                = TopicEngine + "/rpm"
	TopicEngineOilTemperature     = TopicEngine + "/oil-temp"
	TopicEngineOilPressure        = TopicEngine + "/oil-pressure"
	TopicEngineCoolantTemperature = TopicEngine + "/coolant-temp"
	TopicEngineBatteryVoltage     = TopicEngine + "/battery-voltage"
)
