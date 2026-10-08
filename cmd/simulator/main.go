package main

import (
	"context"
	"fmt"
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
	"github.com/sajad-soudani/twin-pipeline/internal"
	"github.com/sajad-soudani/twin-pipeline/internal/ingestion"
	"github.com/sajad-soudani/twin-pipeline/pkg/models"
)

const (
	tickerDelay = time.Second
	assetsCount = 50
)

type SensorGenerator struct {
	Baseline  float64
	Amplitude float64
	Period    time.Duration
	NoiseStd  float64
	startTime time.Time
}

type SensorConfig struct {
	Baseline  float64
	Amplitude float64
	Period    time.Duration
	NoiseStd  float64
	MinAlert  float64
	MaxAlert  float64
}

var DieselGeneratorSensors = map[string]SensorConfig{
	internal.TopicEngineRPM: {
		Baseline: 1500, Amplitude: 15, Period: 2 * time.Minute,
		NoiseStd: 3, MinAlert: 1450, MaxAlert: 1550,
	},
	internal.TopicEngineCoolantTemperature: {
		Baseline: 88, Amplitude: 5, Period: 10 * time.Minute,
		NoiseStd: 0.8, MinAlert: 80, MaxAlert: 100,
	},
	internal.TopicEngineOilPressure: {
		Baseline: 45, Amplitude: 5, Period: 8 * time.Minute,
		NoiseStd: 1.2, MinAlert: 15, MaxAlert: 65,
	},
	internal.TopicEngineOilTemperature: {
		Baseline: 100, Amplitude: 6, Period: 12 * time.Minute,
		NoiseStd: 1.0, MinAlert: 70, MaxAlert: 130,
	},
	internal.TopicEngineExhaustTemperature: {
		Baseline: 480, Amplitude: 60, Period: 5 * time.Minute,
		NoiseStd: 8, MinAlert: 150, MaxAlert: 750,
	},
	internal.TopicEngineBatteryVoltage: {
		Baseline: 13.6, Amplitude: 0.3, Period: 15 * time.Minute,
		NoiseStd: 0.05, MinAlert: 12.0, MaxAlert: 14.8,
	},
	internal.TopicEngineVibration: {
		Baseline: 1.2, Amplitude: 0.3, Period: 3 * time.Minute,
		NoiseStd: 0.15, MinAlert: 0, MaxAlert: 2.8, // B/C boundary
	},
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

// TODO: must be used
// func classifyVibration(v float64) string {
// 	switch {
// 	case v < 1.4:
// 		return "A" // good
// 	case v < 2.8:
// 		return "B" // acceptable
// 	case v < 4.5:
// 		return "C" // unsatisfactory - schedule maintenance
// 	default:
// 		return "D" // danger - shutdown
// 	}
// }

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
			go ingestion.Status(context.Background(), cm, "online")
		},

		OnConnectError: func(err error) {
			slog.Error("OnConnectionError", "error", err.Error())
		},

		ClientConfig: paho.ClientConfig{
			ClientID:      "simulator",
			OnClientError: func(err error) { slog.Error("OnClientError", "error", err.Error()) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				slog.Error("OnServerDisconnect", "reasonCode", d.ReasonCode)
			},
		},
	}

	cmCtx, cmCancel := context.WithCancel(context.Background())
	defer cmCancel()

	cm, cmErr := autopaho.NewConnection(cmCtx, cfg)
	if cmErr != nil {
		slog.Error("Connection Manager Error", "error", cmErr.Error())
		panic(cmErr)
	}

	if err := cm.AwaitConnection(cmCtx); err != nil {
		panic(err)
	}

	expiry := uint32(30)
	ticker := time.NewTicker(tickerDelay)
	defer ticker.Stop()

	assetID := "engine"

	sensors := make(map[string]*SensorGenerator, len(DieselGeneratorSensors))

	for k, v := range DieselGeneratorSensors {
		sensor := NewSensorGenerator(v.Baseline, v.Amplitude, v.NoiseStd, v.Period)
		sensors[k] = sensor
	}

loop:
	for {
		select {
		case <-ticker.C:
			for i := range assetsCount {
				for k, v := range sensors {
					slog.Debug(fmt.Sprintf("#%d for: \" %s \"", i, k))
					go publish(cmCtx, cm, assetID, k, v, expiry)
				}
			}

		case <-ctx.Done():
			slog.Warn("Shutting down...")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)

			ingestion.Status(shutdownCtx, cm, "offline")

			cmCancel()
			cancel()

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
		Retain:  false,
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
