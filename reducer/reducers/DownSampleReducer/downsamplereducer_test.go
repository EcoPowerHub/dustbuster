package downsamplereducer

import (
	"errors"
	"testing"
	"time"

	datapoint "github.com/EcoPowerHub/dustbuster/reducer/point"
)

// Define error variables for consistent error handling
var (
	ErrNoData      = errors.New("no data to reduce")
	ErrInvalidStep = errors.New("invalid step value")
)

// TestDownsampleReducer_Reduce tests the DownsampleReducer's Reduce function.
func TestDownsampleReducer_Reduce(t *testing.T) {
	tests := []struct {
		name        string
		step        int
		data        []datapoint.TimePoint
		want        []datapoint.TimePoint
		wantErr     bool
		expectedErr error
	}{
		{
			name:    "valid step",
			step:    2,
			wantErr: false,
			data: []datapoint.TimePoint{
				{Timestamp: time.Unix(1, 0), Value: 1.0},
				{Timestamp: time.Unix(2, 0), Value: 2.0},
				{Timestamp: time.Unix(3, 0), Value: 3.0},
				{Timestamp: time.Unix(4, 0), Value: 4.0},
			},
			want: []datapoint.TimePoint{
				{Timestamp: time.Unix(1, 0), Value: 1.0},
				{Timestamp: time.Unix(3, 0), Value: 3.0},
				{Timestamp: time.Unix(4, 0), Value: 4.0},
			},
		},
		{
			name:        "empty data",
			step:        2,
			data:        []datapoint.TimePoint{},
			want:        nil,
			wantErr:     true,
			expectedErr: ErrNoData,
		},
		{
			name: "invalid step",
			step: 0,
			data: []datapoint.TimePoint{
				{Timestamp: time.Unix(1, 0), Value: 1.0},
				{Timestamp: time.Unix(2, 0), Value: 2.0},
			},
			want:        nil,
			wantErr:     true,
			expectedErr: ErrInvalidStep,
		},
		{
			name: "step greater than data length",
			step: 5,
			data: []datapoint.TimePoint{
				{Timestamp: time.Unix(1, 0), Value: 1.0},
				{Timestamp: time.Unix(2, 0), Value: 2.0},
				{Timestamp: time.Unix(3, 0), Value: 3.0},
			},
			want: []datapoint.TimePoint{
				{Timestamp: time.Unix(1, 0), Value: 1.0},
				{Timestamp: time.Unix(3, 0), Value: 3.0},
			},
			wantErr: false,
		},
		{
			name: "boundary step",
			step: 1,
			data: []datapoint.TimePoint{
				{Timestamp: time.Unix(1, 0), Value: 1.0},
			},
			want: []datapoint.TimePoint{
				{Timestamp: time.Unix(1, 0), Value: 1.0},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dr := &DownsampleReducer{
				Step: tt.step,
			}
			got, err := dr.Reduce(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Reduce() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && err.Error() != tt.expectedErr.Error() {
				t.Errorf("Reduce() error = %v, expectedErr %v", err, tt.expectedErr.Error())
			}
			if !equalPoints(got, tt.want) {
				t.Errorf("Reduce() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// equalPoints checks if two slices of TimePoint are equal.
func equalPoints(a, b []datapoint.TimePoint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Timestamp != b[i].Timestamp || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}
