package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/timescale/tsbs/load"
	"github.com/valyala/fasthttp"
)

const backingOffChanCap = 100

// allows for testing
var printFn = fmt.Printf

type processor struct {
	backingOffChan chan bool
	backingOffDone chan struct{}
	httpWriter     *HTTPWriter
}

func (p *processor) Init(numWorker int, _ bool) {
	daemonURL := daemonURLs[numWorker%len(daemonURLs)]
	cfg := HTTPWriterConfig{
		DebugInfo: fmt.Sprintf("worker #%d, dest url: %s", numWorker, daemonURL),
		Host:      daemonURL,
		Database:  loader.DatabaseName(),
	}
	w := NewHTTPWriter(cfg, consistency)
	p.initWithHTTPWriter(numWorker, w)
}

func (p *processor) initWithHTTPWriter(numWorker int, w *HTTPWriter) {
	p.backingOffChan = make(chan bool, backingOffChanCap)
	p.backingOffDone = make(chan struct{})
	p.httpWriter = w
	go p.processBackoffMessages(numWorker)
}

func (p *processor) Close(_ bool) {
	close(p.backingOffChan)
	<-p.backingOffDone
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if doLoad {
		var err error
		for {
			if useGzip {
				compressedBatch := bufPool.Get().(*bytes.Buffer)
				fasthttp.WriteGzip(compressedBatch, batch.buf.Bytes())
				_, err = p.httpWriter.WriteLineProtocol(compressedBatch.Bytes(), true)
				// Return the compressed batch buffer to the pool.
				compressedBatch.Reset()
				bufPool.Put(compressedBatch)
			} else {
				_, err = p.httpWriter.WriteLineProtocol(batch.buf.Bytes(), false)
			}

			if err == errBackoff {
				p.backingOffChan <- true
				time.Sleep(backoff)
			} else {
				p.backingOffChan <- false
				break
			}
		}
		if err != nil {
			fatal("Error writing: %s\n", err.Error())
		}
	}
	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return metricCnt, rowCnt
}

func (p *processor) processBackoffMessages(workerID int) {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range p.backingOffChan {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			printFn("[worker %d] backoff took %.02fsec\n", workerID, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	printFn("[worker %d] backoffs took a total of %fsec of runtime\n", workerID, totalBackoffSecs)
	p.backingOffDone <- struct{}{}
}

func (p *processor) CreateAggregatedTable() {
	u, err := url.Parse(p.httpWriter.c.Host)
	if err != nil {
		fatal("Preaggregation error: %s\n", err.Error())
	}
	//DROP PREAGGREGATED DB
	u.Path = "query"
	v := url.Values{}
	v.Set("q", fmt.Sprintf("DROP DATABASE %s_search", p.httpWriter.c.Database))
	u.RawQuery = v.Encode()
	resp, err := http.Post(u.String(), "text/plain", nil)
	if err != nil {
		fatal("Preaggregation error: %s\n", err.Error())
		return
	}
	defer resp.Body.Close()
	ioutil.ReadAll(resp.Body)

	u.Path = "query"
	v = url.Values{}
	v.Set("q", fmt.Sprintf("CREATE DATABASE %s_search", p.httpWriter.c.Database))
	u.RawQuery = v.Encode()
	resp, err = http.Post(u.String(), "text/plain", nil)
	if err != nil {
		fatal("Preaggregation error: %s\n", err.Error())
		return
	}
	defer resp.Body.Close()
	ioutil.ReadAll(resp.Body)

	v = url.Values{}
	v.Set("db", p.httpWriter.c.Database)
	v.Set("q", fmt.Sprintf("SELECT min(value) AS min_value, max(value) AS max_value INTO %s_search.autogen.:MEASUREMENT FROM /sensor_.*/ GROUP BY time(1h)", p.httpWriter.c.Database))
	u.RawQuery = v.Encode()
	resp, err = http.Post(u.String(), "text/plain", nil)
	if err != nil {
		fatal("Preaggregation error: %s\n", err.Error())
		return
	}
	defer resp.Body.Close()
	ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		fatal("Preaggregation error: [%d]%s\n", resp.StatusCode, resp.Status)
		return
	}
}
