package siemens

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

//SampledData produces a filler for queries in the siemens sampled-data case.
type SampledData struct {
	core       utils.QueryGenerator
	interval   time.Duration
	resolution time.Duration
}

//NewSampledData produces a new function that produces a new SampledData
func NewSampledData(interval, resolution time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &SampledData{
			core:       core,
			interval:   interval,
			resolution: resolution,
		}
	}
}

//Fill fills in the query.Query with query details
func (d *SampledData) Fill(q query.Query) query.Query {
	fc, ok := d.core.(SampledDataFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.SampledData(q, d.interval, d.resolution)
	return q
}
