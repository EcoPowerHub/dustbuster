package reducerbuilder

import (
	"fmt"

	"github.com/EcoPowerHub/dustbuster/reducer"
	averagereducer "github.com/EcoPowerHub/dustbuster/reducer/reducers/AverageReducer"
	downsamplereducer "github.com/EcoPowerHub/dustbuster/reducer/reducers/DownSampleReducer"
	maxreducer "github.com/EcoPowerHub/dustbuster/reducer/reducers/MaxReducer"
	minreducer "github.com/EcoPowerHub/dustbuster/reducer/reducers/MinReducer"
	sumreducer "github.com/EcoPowerHub/dustbuster/reducer/reducers/SumReducer"
	"github.com/go-viper/mapstructure/v2"
)

const (
	IdAverageReducer    = "average"
	IdSumReducer        = "sum"
	IdMaxReducer        = "max"
	IdMinReducer        = "min"
	IdDownsampleReducer = "downsample"
)

// reducerRegistry stores the mapping between reducer IDs and their configurations.
var reducerRegistry = map[string]struct {
	config      any
	constructor func(any) (reducer.DataReducer, error)
}{
	IdAverageReducer: {
		config: &averagereducer.Configuration{},
		constructor: func(c any) (reducer.DataReducer, error) {
			conf, ok := c.(*averagereducer.Configuration)
			if !ok {
				return nil, fmt.Errorf("invalid configuration type for average reducer")
			}
			return averagereducer.New(conf)
		},
	},
	IdSumReducer: {
		config: &sumreducer.Configuration{},
		constructor: func(c any) (reducer.DataReducer, error) {
			conf, ok := c.(*sumreducer.Configuration)
			if !ok {
				return nil, fmt.Errorf("invalid configuration type for sum reducer")
			}
			return sumreducer.New(conf)
		},
	},
	IdMaxReducer: {
		config: &maxreducer.Configuration{},
		constructor: func(c any) (reducer.DataReducer, error) {
			conf, ok := c.(*maxreducer.Configuration)
			if !ok {
				return nil, fmt.Errorf("invalid configuration type for max reducer")
			}
			return maxreducer.New(conf)
		},
	},
	IdMinReducer: {
		config: &minreducer.Configuration{},
		constructor: func(c any) (reducer.DataReducer, error) {
			conf, ok := c.(*minreducer.Configuration)
			if !ok {
				return nil, fmt.Errorf("invalid configuration type for min reducer")
			}
			return minreducer.New(conf)
		},
	},
	IdDownsampleReducer: {
		config: &downsamplereducer.Configuration{},
		constructor: func(c any) (reducer.DataReducer, error) {
			conf, ok := c.(*downsamplereducer.Configuration)
			if !ok {
				return nil, fmt.Errorf("invalid configuration type for downsample reducer")
			}
			return downsamplereducer.New(conf)
		},
	},
}

// NewReducer creates a new DataReducer based on the provided id and configuration.
func NewReducer(id string, conf any) (reducer.DataReducer, error) {
	entry, exists := reducerRegistry[id]
	if !exists {
		return nil, fmt.Errorf("unknown reducer id: %s", id)
	}

	if err := decodeConfig(conf, entry.config); err != nil {
		return nil, err
	}

	return entry.constructor(entry.config)
}

// decodeConfig decodes the input configuration into the result using mapstructure.
func decodeConfig(input, result any) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName: "json",
		Result:  result,
	})
	if err != nil {
		return fmt.Errorf("failed to create decoder: %w", err)
	}
	if err := decoder.Decode(input); err != nil {
		return fmt.Errorf("failed to decode configuration: %w", err)
	}
	return nil
}
