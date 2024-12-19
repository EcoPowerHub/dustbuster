package minreducer

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/EcoPowerHub/dustbuster/reducer"
	datapoint "github.com/EcoPowerHub/dustbuster/reducer/point"
)

// New creates a new instance of MinReducer with the provided configuration.
// It parses the interval duration from the configuration and returns an error if the interval is invalid.
//
// Parameters:
//   - conf: Configuration struct containing the interval as a string.
//
// Returns:
//   - *MinReducer: A pointer to the newly created MinReducer instance.
//   - error: An error if the interval parsing fails.
func New(conf *Configuration) (reducer.DataReducer, error) {
	interval, err := time.ParseDuration(conf.Interval)
	if err != nil {
		return nil, fmt.Errorf("invalid interval: %w", err)
	}
	return &MinReducer{
		Interval: interval,
	}, nil
}

// MinReducer reduces data by keeping the minimum value over fixed intervals.
type MinReducer struct {
	Interval time.Duration
}

// Reduce processes a slice of TimePoint data and reduces it by finding the minimum value
// within each interval specified by the MinReducer's Interval field. It returns a slice
// of TimePoint containing the reduced data.
//
// Parameters:
//   - data: A slice of TimePoint to be reduced.
//
// Returns:
//   - A slice of TimePoint containing the reduced data.
//   - An error if the input data slice is empty.
//
// The function iterates over the input data and groups the points by the specified interval.
// For each interval, it finds the minimum value and appends a new TimePoint with the start
// time of the interval and the minimum value to the result slice.
func (mr *MinReducer) Reduce(data []datapoint.TimePoint) ([]datapoint.TimePoint, error) {
	if len(data) == 0 {
		return nil, errors.New("no data to reduce")
	}
	var reduced []datapoint.TimePoint
	startTime := data[0].Timestamp
	minValue := math.MaxFloat64

	for _, point := range data {
		if point.Timestamp.Before(startTime.Add(mr.Interval)) {
			if point.Value < minValue {
				minValue = point.Value
			}
		} else {
			reduced = append(reduced, datapoint.TimePoint{
				Timestamp: startTime,
				Value:     minValue,
			})
			startTime = startTime.Add(mr.Interval)
			minValue = point.Value
		}
	}

	if minValue < math.MaxFloat64 {
		reduced = append(reduced, datapoint.TimePoint{
			Timestamp: startTime,
			Value:     minValue,
		})
	}

	return reduced, nil
}
