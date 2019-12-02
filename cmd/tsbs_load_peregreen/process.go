package main

import (
	"bytes"
	"github.com/timescale/tsbs/load"
	"github.com/visheratin/tss/data"
	"log"
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
	for sensor, elements := range btch.m {
		elsCommon := data.Elements{Type: float64ElType, F64: elements}
		marshaledEls, _ := elsCommon.MarshalMsg(nil)
		buf := bytes.NewBuffer(marshaledEls)
		// buf contains all elements from one batch for the same sensor
		rowCnt += len(elements)
		metricCnt += len(elements)
		resp, err := http.Post(protocol+host+":"+port+"/upload/"+sensor+"/msgp/format/float64",
			contentType, buf)
		if err != nil {
			fatal(err.Error())
		}
		log.Printf("resp status: %s sensor: %s\n", resp.Status, sensor)
		if resp.StatusCode != 200 {
			log.Println(resp.Status)
		}
	}
	return uint64(metricCnt), uint64(rowCnt)
}
