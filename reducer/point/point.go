package datapoint

import "time"

// TimePoint represents a single point in a time series.
type TimePoint struct {
	Timestamp time.Time // Unix timestamp
	Value     float64   // Value at the given timestamp
}
