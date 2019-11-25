package siemens

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

//Search produces a filler for queries in the siemens search case.
type Search struct {
	core     utils.QueryGenerator
	interval time.Duration
}

//NewSearch produces a new function that produces a new Search
func NewSearch(interval time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &Search{
			core:     core,
			interval: interval,
		}
	}
}

//Fill fills in the query.Query with query details
func (d *Search) Fill(q query.Query) query.Query {
	fc, ok := d.core.(SearchFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.Search(q, d.interval)
	return q
}
