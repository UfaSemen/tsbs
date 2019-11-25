package siemens

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

//Maximum produces a filler for queries in the siemens maximum case.
type Maximum struct {
	core       utils.QueryGenerator
	interval   time.Duration
	resolution time.Duration
}

//MaximumData produces a new function that produces a new MaximumData
func NewMaximum(interval, resolution time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &Maximum{
			core:       core,
			interval:   interval,
			resolution: resolution,
		}
	}
}

//Fill fills in the query.Query with query details
func (d *Maximum) Fill(q query.Query) query.Query {
	fc, ok := d.core.(MaximumFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.Maximum(q, d.interval, d.resolution)
	return q
}
