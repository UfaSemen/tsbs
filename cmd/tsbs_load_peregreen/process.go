package main

import (
	"bytes"
	"github.com/timescale/tsbs/load"
	"github.com/visheratin/tss/data"
	"net/http"
)

type processor struct{}

const (
	protocol      = "http://"
	float64ElType = 6
	contentType   = "application/octet-stream"
)

func (p *processor) Init(workerNum int, doLoad bool) {}

func (p *processor) ProcessBatch(b load.Batch, _ bool) (uint64, uint64) {
	btch := b.(*batch)
	var rowCnt int
	var metricCnt int
	elsCommon := data.Elements{Type: float64ElType, F64: btch.m}
	marshaledEls, _ := elsCommon.MarshalMsg(nil)
	buf := bytes.NewBuffer(marshaledEls)
	// buf contains all elements from one batch for the same sensor
	rowCnt += len(btch.m)
	metricCnt += len(btch.m)
	_, err := http.Post(protocol+host+":"+port+"/upload/"+btch.sensor+"/msgp/format/float64",
		contentType, buf)
	if err != nil {
		fatal(err.Error())
	}

	return uint64(metricCnt), uint64(rowCnt)
}
