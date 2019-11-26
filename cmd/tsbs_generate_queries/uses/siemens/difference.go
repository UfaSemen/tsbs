package siemens

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

//Difference produces a filler for queries in the siemens difference case.
type Difference struct {
	core     utils.QueryGenerator
	interval time.Duration
}

//Difference produces a new function that produces a new Difference
func NewDifference(interval time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &Difference{
			core:     core,
			interval: interval,
		}
	}
}

//Fill fills in the query.Query with query details
func (d *Difference) Fill(q query.Query) query.Query {
	fc, ok := d.core.(DifferenceFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.Difference(q, d.interval)
	return q
}
