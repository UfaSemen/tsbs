package siemens

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

//RawData produces a filler for queries in the siemens raw-data case.
type RawData struct {
	core     utils.QueryGenerator
	interval time.Duration
}

//NewRawData produces a new function that produces a new RawData
func NewRawData(interval time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &RawData{
			core:     core,
			interval: interval,
		}
	}
}

//Fill fills in the query.Query with query details
func (d *RawData) Fill(q query.Query) query.Query {
	fc, ok := d.core.(RawDataFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.RawData(q, d.interval)
	return q
}
