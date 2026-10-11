package worker

import "github.com/eclipse/paho.golang/paho"

type Job struct {
	Topic      string
	QoS        byte
	Payload    []byte
	Retain     bool
	Properties paho.UserProperties
}
