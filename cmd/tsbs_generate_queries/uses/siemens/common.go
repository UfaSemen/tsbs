package siemens

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/query"
)

const (
	LabelRawData       = "raw-data"
	LabelSearch        = "search"
	LabelSampledData   = "sampled-data"
	LabelMaximum       = "maximum"
	LabelMovingAverage = "moving-average"
	LabelHour          = "-hour"
	LabelDay           = "-day"
	LabelWeek          = "-week"
	LabelMonth         = "-month"
	LabelYear          = "-year"
)

// Core is the common component of all generators for all systems
type Core struct {
	*common.Core
}

// NewCore returns a new Core for the given time range and cardinality
func NewCore(start, end time.Time, scale int) (*Core, error) {
	c, err := common.NewCore(start, end, scale)
	return &Core{Core: c}, err
}

// RawDataFiller is a type that can fill in a raw data query
type RawDataFiller interface {
	RawData(query.Query, time.Duration)
}

// SearchFiller is a type that can fill in a search query
type SearchFiller interface {
	Search(query.Query, time.Duration)
}

// SampledDataFiller is a type that can fill in a sampled data query
type SampledDataFiller interface {
	SampledData(query.Query, time.Duration, time.Duration)
}

// MaximumFiller is a type that can fill in a maximum query
type MaximumFiller interface {
	Maximum(query.Query, time.Duration, time.Duration)
}

// MovingAverageFiller is a type that can fill in a moving average query
type MovingAverageFiller interface {
	MovingAverage(query.Query, time.Duration, time.Duration)
}
