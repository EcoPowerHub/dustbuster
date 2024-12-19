package sumreducer

import (
	"testing"
	"time"

	datapoint "github.com/EcoPowerHub/dustbuster/reducer/point"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	conf := Configuration{Interval: "1m"}
	reducer, err := New(&conf)
	assert.NoError(t, err)
	assert.NotNil(t, reducer)

	conf = Configuration{Interval: "invalid"}
	reducer, err = New(&conf)
	assert.Error(t, err)
	assert.Nil(t, reducer)
}

func TestReduce(t *testing.T) {
	conf := Configuration{Interval: "1m"}
	reducer, err := New(&conf)
	assert.NoError(t, err)
	assert.NotNil(t, reducer)

	sumReducer := reducer.(*SumReducer)

	data := []datapoint.TimePoint{
		{Timestamp: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC), Value: 1.0},
		{Timestamp: time.Date(2023, 10, 1, 0, 0, 30, 0, time.UTC), Value: 2.0},
		{Timestamp: time.Date(2023, 10, 1, 0, 1, 0, 0, time.UTC), Value: 3.0},
		{Timestamp: time.Date(2023, 10, 1, 0, 1, 30, 0, time.UTC), Value: 4.0},
	}

	expected := []datapoint.TimePoint{
		{Timestamp: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC), Value: 3.0},
		{Timestamp: time.Date(2023, 10, 1, 0, 1, 0, 0, time.UTC), Value: 7.0},
	}

	reduced, err := sumReducer.Reduce(data)
	assert.NoError(t, err)
	assert.Equal(t, expected, reduced)

	// Test with empty data
	reduced, err = sumReducer.Reduce([]datapoint.TimePoint{})
	assert.Error(t, err)
	assert.Nil(t, reduced)
}
