package minreducer

import (
	"testing"
	"time"

	datapoint "github.com/EcoPowerHub/dustbuster/reducer/point"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name      string
		conf      Configuration
		expectErr bool
	}{
		{
			name: "valid interval",
			conf: Configuration{Interval: "1m"},
		},
		{
			name:      "invalid interval",
			conf:      Configuration{Interval: "invalid"},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(&tt.conf)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestMinReducer_Reduce(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		interval  string
		data      []datapoint.TimePoint
		expected  []datapoint.TimePoint
		expectErr bool
	}{
		{
			name:      "empty data",
			interval:  "1m",
			data:      []datapoint.TimePoint{},
			expected:  nil,
			expectErr: true,
		},
		{
			name:     "single interval",
			interval: "1m",
			data: []datapoint.TimePoint{
				{Timestamp: time.Unix(0, 0), Value: 10},
				{Timestamp: time.Unix(30, 0), Value: 5},
				{Timestamp: time.Unix(60, 0), Value: 20},
			},
			expected: []datapoint.TimePoint{
				{Timestamp: time.Unix(0, 0), Value: 5},
				{Timestamp: time.Unix(60, 0), Value: 20},
			},
		},
		{
			name:     "multiple intervals",
			interval: "1m",
			data: []datapoint.TimePoint{
				{Timestamp: time.Unix(0, 0), Value: 10},
				{Timestamp: time.Unix(30, 0), Value: 5},
				{Timestamp: time.Unix(90, 0), Value: 20},
				{Timestamp: time.Unix(120, 0), Value: 15},
			},
			expected: []datapoint.TimePoint{
				{Timestamp: time.Unix(0, 0), Value: 5},
				{Timestamp: time.Unix(60, 0), Value: 20},
				{Timestamp: time.Unix(120, 0), Value: 15},
			},
		},
		{
			name:     "interval larger than dataset range",
			interval: "5m",
			data: []datapoint.TimePoint{
				{Timestamp: time.Unix(0, 0), Value: 10},
				{Timestamp: time.Unix(60, 0), Value: 5},
				{Timestamp: time.Unix(120, 0), Value: 20},
			},
			expected: []datapoint.TimePoint{
				{Timestamp: time.Unix(0, 0), Value: 5},
			},
		},
		{
			name:     "large interval with single point",
			interval: "10m",
			data: []datapoint.TimePoint{
				{Timestamp: time.Unix(0, 0), Value: 42},
			},
			expected: []datapoint.TimePoint{
				{Timestamp: time.Unix(0, 0), Value: 42},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr, err := New(&Configuration{Interval: tt.interval})
			assert.NoError(t, err)
			result, err := mr.Reduce(tt.data)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
