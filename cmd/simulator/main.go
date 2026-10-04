package main

import (
	"context"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/sajad-soudani/twin-pipeline/pkg/models"
)

const (
	topicStatus            = "twin/status"
	topicData              = "twin/sensors"
	topicEngineTemperature = "twin/sensors/engine/temperature"
	noise                  = 0.5
	tickerDelay            = 2 * time.Second
)

type SensorGenerator struct {
	Baseline  float64
	Amplitude float64
	Period    time.Duration
	NoiseStd  float64
	startTime time.Time
}

func NewSensorGenerator(baseline, amplitude, noisestd float64, period time.Duration) *SensorGenerator {
	return &SensorGenerator{
		Baseline:  baseline,
		Amplitude: amplitude,
		Period:    period,
		NoiseStd:  noisestd,
		startTime: time.Now(),
	}
}

func (s *SensorGenerator) Next() float64 {
	elapsed := time.Since(s.startTime).Seconds()
	periodSeconds := s.Period.Seconds()

	angel := 2 * math.Pi * (elapsed / periodSeconds)
	wave := s.Amplitude * math.Sin(angel)

	noise := rand.NormFloat64() * s.NoiseStd

	return s.Baseline + wave + noise
}

func main() {
	var handler slog.Handler
	if os.Getenv("ENV") == "production" {
		handler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	u, uErr := url.Parse("mqtt://127.0.0.1:1883")
	if uErr != nil {
		slog.Error("MQTT URL parsing error", "error", uErr.Error())
	}

	cfg := autopaho.ClientConfig{
		ServerUrls: []*url.URL{u},
		KeepAlive:  20,

		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         60,

		OnConnectionUp: func(cm *autopaho.ConnectionManager, c *paho.Connack) {
			slog.Info("Connection Up", "isSessionPresent", c.SessionPresent)
		},

		OnConnectError: func(err error) {
			slog.Error("OnConnectionError", "error", err.Error())
		},

		ClientConfig: paho.ClientConfig{
			ClientID:      "simulator-publisherr", // must differ from the publisher's ID
			OnClientError: func(err error) { slog.Error("OnClientError", "error", err.Error()) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				slog.Error("OnServerDisconnect", "reasonCode", d.ReasonCode)
			},
		},
	}

	cm, cmErr := autopaho.NewConnection(ctx, cfg)
	if cmErr != nil {
		slog.Error("Connection Manager Error", "error", cmErr.Error())
		panic(cmErr)
	}

	if err := cm.AwaitConnection(ctx); err != nil {
		panic(err)
	}

	expiry := uint32(30)
	ticker := time.NewTicker(tickerDelay)
	defer ticker.Stop()

	n := 0

	tempSensor := NewSensorGenerator(57, 30, noise, time.Minute*5)
	assetID := "engine"

loop:
	for {
		select {
		case <-ticker.C:
			n++
			go publish(ctx, cm, assetID, topicEngineTemperature, tempSensor, expiry)

		case <-ctx.Done():
			slog.Warn("Shutting down...")
			<-cm.Done()
			break loop
		}
	}

}

func publish(ctx context.Context, cm *autopaho.ConnectionManager, assetID, topic string, sensorSrc *SensorGenerator, expiry uint32) {
	sensor, sensorErr := models.Marshal(assetID, "temperature", sensorSrc.Next())
	if sensorErr != nil {
		slog.Error("Marshal Error", "error", sensorErr.Error())
	}
	_, pubErr := cm.Publish(ctx, &paho.Publish{
		Topic:   topic,
		QoS:     1,
		Payload: sensor,
		Retain:  true,
		Properties: &paho.PublishProperties{
			MessageExpiry: &expiry,
			ContentType:   "application/json",
			User: paho.UserProperties{
				{Key: "sender", Value: "simulator"},
			},
		},
	})
	if pubErr != nil && ctx.Err() == nil {
		slog.Error("Publish Error", "error", pubErr.Error())
	}
}
