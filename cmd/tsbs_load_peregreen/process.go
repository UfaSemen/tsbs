package main

import (
	"github.com/timescale/tsbs/load"
	"log"
	"net/http"
	"net/url"
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
	for sensor, data := range btch.m {
		//elsCommon := data.Elements{Type: float64ElType, F64: elements}
		//marshaledEls, _ := elsCommon.MarshalMsg(nil)
		//buf := bytes.NewBuffer(marshaledEls)
		// buf contains all elements from one batch for the same sensor
		rowCnt += data.len
		metricCnt += data.len
		format := "2-1- "
		format = url.PathEscape(format)
		path := protocol + host + ":" + port + "/upload/" + sensor + "/csv/" + format + "/float64"
		resp, err := http.Post(path, contentType, &data.buf)
		if err != nil {
			fatal(err.Error())
		}
		if resp.StatusCode != 200 {
			log.Println(resp.Status)
			log.Fatal("resp.Status != 200!")
		}
	}

	return uint64(metricCnt), uint64(rowCnt)
}
