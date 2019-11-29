package main

import (
	"bytes"
	"github.com/visheratin/tss/data"
	"net/http"

	"github.com/timescale/tsbs/load"
)

type processor struct{}

const protocol = "http://"

func (p *processor) Init(workerNum int, doLoad bool) {}

func (p *processor) ProcessBatch(b load.Batch, _ bool) (uint64, uint64) {
	btch := b.(*batch)
	var rowCnt int
	var metricCnt int
	for sensor, elements := range btch.m {
		elsCommon := data.Elements{Type: 6, F64: elements}
		marshaledEls, _ := elsCommon.MarshalMsg(nil)
		buf := bytes.NewBuffer(marshaledEls)
		// buf contains all elements from one batch for the same sensor
		rowCnt += len(elements)
		metricCnt += len(elements)
		_, err := http.Post(protocol+host+":"+port+"/upload/"+sensor+"/msgp/format/float64",
			"application/octet-stream", buf)
		if err != nil {
			fatal(err.Error())
		}
	}
	return uint64(metricCnt), uint64(rowCnt)
}
