package sumreducer

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/EcoPowerHub/dustbuster/reducer"
	datapoint "github.com/EcoPowerHub/dustbuster/reducer/point"
)

func New(conf *Configuration) (reducer.DataReducer, error) {
	interval, err := time.ParseDuration(conf.Interval)
	if err != nil {
		return nil, fmt.Errorf("invalid interval: %w", err)
	}

	if interval <= 0 {
		return nil, fmt.Errorf("interval must be positive, got %v", interval)
	}

	return &SumReducer{
		Interval: interval,
	}, nil
}

type SumReducer struct {
	Interval time.Duration
}

func (sr *SumReducer) Reduce(data []datapoint.TimePoint) ([]datapoint.TimePoint, error) {
	if len(data) == 0 {
		return nil, errors.New("no data to reduce")
	}

	// Sort data by timestamp to handle unsorted input
	sort.Slice(data, func(i, j int) bool {
		return data[i].Timestamp.Before(data[j].Timestamp)
	})

	var reduced []datapoint.TimePoint
	var sum float64
	startTime := data[0].Timestamp

	for _, point := range data {
		if point.Timestamp.Before(startTime.Add(sr.Interval)) {
			sum += point.Value
		} else {
			// Append the summed value for the current interval
			reduced = append(reduced, datapoint.TimePoint{
				Timestamp: startTime,
				Value:     sum,
			})
			// Move to the next interval
			for startTime.Add(sr.Interval).Before(point.Timestamp) {
				startTime = startTime.Add(sr.Interval)
				reduced = append(reduced, datapoint.TimePoint{
					Timestamp: startTime,
					Value:     0,
				})
			}
			// Start accumulating for the new interval
			startTime = startTime.Add(sr.Interval)
			sum = point.Value
		}
	}

	// Append the final interval's sum
	if sum > 0 {
		reduced = append(reduced, datapoint.TimePoint{
			Timestamp: startTime,
			Value:     sum,
		})
	}

	return reduced, nil
}
