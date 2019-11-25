package siemens

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

//MovingAverage produces a filler for queries in the siemens moving average case.
type MovingAverage struct {
	core       utils.QueryGenerator
	interval   time.Duration
	resolution time.Duration
}

//MovingAverage produces a new function that produces a new MovingAverage
func NewMovingAverage(interval, resolution time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &MovingAverage{
			core:       core,
			interval:   interval,
			resolution: resolution,
		}
	}
}

//Fill fills in the query.Query with query details
func (d *MovingAverage) Fill(q query.Query) query.Query {
	fc, ok := d.core.(MovingAverageFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.MovingAverage(q, d.interval, d.resolution)
	return q
}
