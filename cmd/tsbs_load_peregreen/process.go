package main

import (
	"bytes"
	"github.com/timescale/tsbs/load"
	"log"
	"net/http"
)

type processor struct{}

func (p *processor) Init(workerNum int, doLoad bool) {}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	btch := b.(batch)
	var rowCnt int
	var metricCnt int
	for sensor, elements := range btch.m {
		marshaledEls, _ := elements.MarshalMsg(nil)
		buf := bytes.NewBuffer(marshaledEls)
		// buf contains all elements from one batch for the same sensor
		rowCnt += len(elements)
		metricCnt += len(elements)
		_, err := http.Post("/upload/"+sensor+"/"+"msgp"+"/"+"0-1- "+"/"+"float64",
			"text/plain", buf)
		if err != nil {
			log.Fatal(err)
		}
	}
	return uint64(metricCnt), uint64(rowCnt)
}
