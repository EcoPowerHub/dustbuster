package maxreducer

import (
	"testing"
	"time"

	datapoint "github.com/EcoPowerHub/dustbuster/reducer/point"
	"github.com/stretchr/testify/assert"
)

func TestMaxReducer_Reduce(t *testing.T) {
	tests := []struct {
		name     string
		interval string
		data     []datapoint.TimePoint
		expected []datapoint.TimePoint
		wantErr  bool
	}{
		{
			name:     "empty data",
			interval: "1m",
			data:     []datapoint.TimePoint{},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "single point",
			interval: "1m",
			data: []datapoint.TimePoint{
				{Timestamp: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC), Value: 10},
			},
			expected: []datapoint.TimePoint{
				{Timestamp: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC), Value: 10},
			},
			wantErr: false,
		},
		{
			name:     "multiple points within interval",
			interval: "1m",
			data: []datapoint.TimePoint{
				{Timestamp: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC), Value: 10},
				{Timestamp: time.Date(2023, 10, 1, 0, 0, 30, 0, time.UTC), Value: 20},
			},
			expected: []datapoint.TimePoint{
				{Timestamp: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC), Value: 20},
			},
			wantErr: false,
		},
		{
			name:     "multiple points across intervals",
			interval: "1m",
			data: []datapoint.TimePoint{
				{Timestamp: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC), Value: 10},
				{Timestamp: time.Date(2023, 10, 1, 0, 0, 30, 0, time.UTC), Value: 20},
				{Timestamp: time.Date(2023, 10, 1, 0, 1, 0, 0, time.UTC), Value: 15},
				{Timestamp: time.Date(2023, 10, 1, 0, 1, 30, 0, time.UTC), Value: 25},
			},
			expected: []datapoint.TimePoint{
				{Timestamp: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC), Value: 20},
				{Timestamp: time.Date(2023, 10, 1, 0, 1, 0, 0, time.UTC), Value: 25},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := Configuration{Interval: tt.interval}
			reducer, err := New(&conf)
			assert.NoError(t, err)

			mr := reducer.(*MaxReducer)
			result, err := mr.Reduce(tt.data)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
