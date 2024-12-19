package maxreducer

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/EcoPowerHub/dustbuster/reducer"
	datapoint "github.com/EcoPowerHub/dustbuster/reducer/point"
)

func New(conf *Configuration) (reducer.DataReducer, error) {
	interval, err := time.ParseDuration(conf.Interval)
	if err != nil {
		return nil, fmt.Errorf("invalid interval: %w", err)
	}
	return &MaxReducer{
		Interval: interval,
	}, nil
}

// MaxReducer reduces data by keeping the maximum value over fixed intervals.
type MaxReducer struct {
	Interval time.Duration
}

// Reduce processes time series data and returns maximum values for each interval.
// Time complexity: O(n) where n is the number of data points.
// Assumes input data points are sorted by timestamp in ascending order.
func (mr *MaxReducer) Reduce(data []datapoint.TimePoint) ([]datapoint.TimePoint, error) {
	if len(data) == 0 {
		return nil, errors.New("no data to reduce")
	}
	// Validate timestamps are in order
	for i := 1; i < len(data); i++ {
		if data[i].Timestamp.Before(data[i-1].Timestamp) {
			return nil, errors.New("data points must be sorted by timestamp")
		}
	}
	// Pre-allocate slice with estimated capacity
	estimatedSize := len(data)/2 + 1
	reduced := make([]datapoint.TimePoint, 0, estimatedSize)
	startTime := data[0].Timestamp
	maxValue := -math.MaxFloat64
	for _, point := range data {
		if point.Timestamp.Before(startTime.Add(mr.Interval)) {
			if point.Value > maxValue {
				maxValue = point.Value
			}
		} else {
			reduced = append(reduced, datapoint.TimePoint{
				Timestamp: startTime,
				Value:     maxValue,
			})
			startTime = startTime.Add(mr.Interval)
			maxValue = point.Value
		}
	}
	if maxValue > -math.MaxFloat64 {
		reduced = append(reduced, datapoint.TimePoint{
			Timestamp: startTime,
			Value:     maxValue,
		})
	}
	return reduced, nil
}
