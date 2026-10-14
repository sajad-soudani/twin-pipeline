package topics

import "testing"

func TestParseSensor(t *testing.T) {
	tests := []struct {
		topic, asset, metric string
		wantErr              bool
	}{
		{"twin/sensors/engine-001/rpm", "engine-001", "rpm", false},
		{"twin/sensors/a.b_c/exhaust-temp", "a.b_c", "exhaust-temp", false},
		{"twin/status/simulator", "", "", true},
		{"twin/sensors/engine-001", "", "", true},           // no metric
		{"twin/sensors/engine-001/rpm/extra", "", "", true}, // too deep
		{"twin/sensors//rpm", "", "", true},                 // empty asset
		{"twin/sensors/en gine/rpm", "", "", true},          // bad char
		{"twin/sensors/engine/+", "", "", true},             // wildcard
	}
	for _, tc := range tests {
		a, m, err := ParseSensor(tc.topic)
		if (err != nil) != tc.wantErr {
			t.Fatalf("%q: err=%v wantErr=%v", tc.topic, err, tc.wantErr)
		}
		if a != tc.asset || m != tc.metric {
			t.Errorf("%q: got (%q,%q) want (%q,%q)", tc.topic, a, m, tc.asset, tc.metric)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	a, m, err := ParseSensor(Sensor("engine-007", "oil-temp"))
	if err != nil || a != "engine-007" || m != "oil-temp" {
		t.Fatalf("round trip failed: %q %q %v", a, m, err)
	}
}

func TestSegmentLengthLimit(t *testing.T) {
	long := make([]byte, maxSegmentLen+1)
	for i := range long {
		long[i] = 'a'
	}
	if _, _, err := ParseSensor(Sensor(string(long), "rpm")); err == nil {
		t.Fatal("expected error for over-long asset id")
	}
}
