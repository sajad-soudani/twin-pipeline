package model

import (
	"errors"
	"testing"
	"time"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	ts := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	b, err := EncodeReading(42.5, ts)
	if err != nil {
		t.Fatal(err)
	}
	r, err := DecodeReading(b)
	if err != nil {
		t.Fatal(err)
	}
	if r.Value != 42.5 || !r.Timestamp.Equal(ts) {
		t.Fatalf("got %+v", r)
	}
}

func TestDecodeRejectsBadInput(t *testing.T) {
	for name, payload := range map[string]string{
		"not json":          `nope`,
		"empty":             ``,
		"missing timestamp": `{"value": 1}`,
		"wrong type":        `{"value": "hot", "timestamp": "2026-10-11T12:00:00Z"}`,
	} {
		_, err := DecodeReading([]byte(payload))
		if !errors.Is(err, ErrInvalidReading) {
			t.Errorf("%s: want ErrInvalidReading, got %v", name, err)
		}
	}
}
