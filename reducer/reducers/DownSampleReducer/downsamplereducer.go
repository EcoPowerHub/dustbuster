package downsamplereducer

import (
	"errors"

	"github.com/EcoPowerHub/dustbuster/reducer"
	datapoint "github.com/EcoPowerHub/dustbuster/reducer/point"
)

func New(conf *Configuration) (reducer.DataReducer, error) {
	if conf == nil {
		return nil, errors.New("configuration cannot be nil")
	}
	if conf.Step <= 0 {
		return nil, errors.New("step must be greater than zero")
	}
	return &DownsampleReducer{
		Step: conf.Step,
	}, nil
}

// DownsampleReducer reduces data by selecting every Nth point.
type DownsampleReducer struct {
	Step int
}

// Reduce downsamples the given slice of TimePoint data by selecting every nth element,
// where n is specified by the Step field of the DownsampleReducer. If the input data
// slice is empty, or if the Step value is less than or equal to zero, an error is returned.
//
// Parameters:
//   - data: A slice of TimePoint to be downsampled.
//
// Returns:
//   - A slice of downsampled TimePoint.
//   - An error if the input data is empty or the Step value is invalid.
func (dr *DownsampleReducer) Reduce(data []datapoint.TimePoint) ([]datapoint.TimePoint, error) {
	if len(data) == 0 {
		return nil, errors.New("no data to reduce")
	}
	if dr.Step <= 0 {
		return nil, errors.New("invalid step value")
	}

	// Protect against integer overflow in capacity calculation
	if len(data) > (1<<31-1)/2 {
		return nil, errors.New("input data too large")
	}

	// Pre-allocate capacity for better performance
	capacity := (len(data) + dr.Step - 1) / dr.Step
	reduced := make([]datapoint.TimePoint, 0, capacity)

	// Always include the first point
	reduced = append(reduced, data[0])

	// Process intermediate points
	for i := dr.Step; i < len(data); i += dr.Step {
		reduced = append(reduced, data[i])
	}

	// Always include the last point if it's not already included
	lastIdx := len(data) - 1
	if lastIdx > 0 && lastIdx%dr.Step != 0 {
		reduced = append(reduced, data[len(data)-1])
	}

	return reduced, nil
}
