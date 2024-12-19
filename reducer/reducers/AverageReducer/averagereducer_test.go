package averagereducer

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

func TestReduce(t *testing.T) {
	tests := []struct {
		name      string
		interval  time.Duration
		data      []datapoint.TimePoint
		expected  []datapoint.TimePoint
		expectErr bool
	}{
		{
			name:      "empty data",
			interval:  time.Minute,
			data:      []datapoint.TimePoint{},
			expected:  nil,
			expectErr: true,
		},
		{
			name:     "single interval",
			interval: time.Minute,
			data: []datapoint.TimePoint{
				{Timestamp: time.Unix(0, 0), Value: 1},
				{Timestamp: time.Unix(30, 0), Value: 2},
			},
			expected: []datapoint.TimePoint{
				{Timestamp: time.Unix(0, 0), Value: 1.5},
			},
		},
		{
			name:     "multiple intervals",
			interval: time.Minute,
			data: []datapoint.TimePoint{
				{Timestamp: time.Unix(0, 0), Value: 1},
				{Timestamp: time.Unix(30, 0), Value: 2},
				{Timestamp: time.Unix(60, 0), Value: 3},
				{Timestamp: time.Unix(90, 0), Value: 4},
			},
			expected: []datapoint.TimePoint{
				{Timestamp: time.Unix(0, 0), Value: 1.5},
				{Timestamp: time.Unix(60, 0), Value: 3.5},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ar := &AverageReducer{Interval: tt.interval}
			result, err := ar.Reduce(tt.data)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
