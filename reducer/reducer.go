package reducer

import (
	datapoint "github.com/EcoPowerHub/dustbuster/reducer/point"
)

// DataReducer defines an interface for reducing and summarizing time series data.
type DataReducer interface {
	Reduce(data []datapoint.TimePoint) ([]datapoint.TimePoint, error)
}
