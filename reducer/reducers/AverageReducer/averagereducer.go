package averagereducer

import (
	"errors"
	"fmt"
	"time"

	"github.com/EcoPowerHub/dustbuster/reducer"
	datapoint "github.com/EcoPowerHub/dustbuster/reducer/point"
)

// New creates a new instance of AverageReducer with the provided configuration.
// It parses the interval duration from the configuration and returns an error if the interval is invalid.
//
// Parameters:
//   - conf: Configuration struct containing the interval as a string.
//
// Returns:
//   - *AverageReducer: A pointer to the newly created AverageReducer instance.
//   - error: An error if the interval parsing fails.
func New(conf *Configuration) (reducer.DataReducer, error) {
	interval, err := time.ParseDuration(conf.Interval)
	if err != nil {
		return nil, fmt.Errorf("invalid interval: %w", err)
	}
	if interval <= 0 {
		return nil, fmt.Errorf("interval must be positive, got %v", interval)
	}
	return &AverageReducer{
		Interval: interval,
	}, nil
}

// AverageReducer reduces data by calculating the average over fixed intervals.
type AverageReducer struct {
	Interval time.Duration // Interval in seconds
}

// Reduce takes a slice of TimePoint data and reduces it by averaging the values
// over intervals defined by the AverageReducer's Interval field. It returns a
// slice of reduced TimePoint data or an error if the input data is empty.
//
// Parameters:
//
//	data []datapoint.TimePoint - A slice of TimePoint data to be reduced.
//
// Returns:
//
//	[]datapoint.TimePoint - A slice of reduced TimePoint data.
//	error - An error if the input data is empty.
func (ar *AverageReducer) Reduce(data []datapoint.TimePoint) ([]datapoint.TimePoint, error) {
	if len(data) == 0 {
		return nil, errors.New("no data to reduce")
	}
	var reduced []datapoint.TimePoint
	var sum float64
	var count int64
	startTime := data[0].Timestamp

	for _, point := range data {
		if point.Timestamp.Before(startTime.Add(ar.Interval)) {
			sum += point.Value
			count++
		} else {
			reduced = append(reduced, datapoint.TimePoint{
				Timestamp: startTime,
				Value:     sum / float64(count),
			})
			startTime = startTime.Add(ar.Interval)
			sum = point.Value
			count = 1
		}
	}

	if count > 0 {
		reduced = append(reduced, datapoint.TimePoint{
			Timestamp: startTime,
			Value:     sum / float64(count),
		})
	}

	return reduced, nil
}
