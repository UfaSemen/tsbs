package main

//go:generate msgp

import (
	"bufio"
	"strconv"
	"strings"

	"github.com/timescale/tsbs/load"
	"github.com/visheratin/tss/data"
)

// sensorIndexer is used to consistently send the same sensor to the same worker
type sensorIndexer struct {
	partitions uint
}

func (i *sensorIndexer) GetIndex(item *load.Point) int {
	p := item.Data.(*point)
	// sensor name starts from 8th symbol. improved hash func to avoid collisions
	s, _ := strconv.Atoi(p.sensor[7:])
	return s % int(i.partitions)
}

type factory struct{}

type point struct {
	sensor  string
	element data.Float64Element
}

func (f *factory) New() load.Batch {
	return &batch{
		m:   map[string]data.Float64Elements{},
		cnt: 0,
	}
}

type batch struct {
	m   map[string]data.Float64Elements
	cnt int
}

func (b *batch) Len() int {
	return b.cnt
}

func (b *batch) Append(item *load.Point) {
	this := item.Data.(*point)

	s := this.sensor
	b.m[s] = append(b.m[s], this.element)
	b.cnt++
}

type decoder struct {
	scanner    bufio.Scanner
	workNum    int
	batchSize  int
	worker     int
	numInBatch int
	n          int
	end        int
	readStrs   []string
}

func (d *decoder) Decode(bf *bufio.Reader) *load.Point {
	var p point
	if d.worker == 0 {
		for i := 0; i < workNum; i++ {
			ok := d.scanner.Scan()
			if !ok && d.scanner.Err() == nil {
				if d.numInBatch == 0 {
					return nil
				}
				d.end = d.numInBatch
				d.worker = (d.worker + 1) % d.workNum
				d.numInBatch = 0
				d.n = 0
				break
			} else if !ok {
				fatal("scan error: %v", d.scanner.Err())
				return nil
			}
			//d.readStrs[d.n] = make([]string, len(d.scanner.Text()))
			//copy(d.readStrs[d.n], d.scanner.Text())
			d.readStrs[d.n] = d.scanner.Text()
			d.n++
		}
	}
	p = d.parsePoint((d.readStrs[d.numInBatch*d.workNum+d.worker]))
	d.numInBatch++
	if d.numInBatch == d.batchSize || d.numInBatch == d.end {
		d.worker = (d.worker + 1) % d.workNum
		d.numInBatch = 0
		if d.worker == 0 {
			d.n = 0
		}
	}
	return load.NewPoint(&p)
}

func (d *decoder) parsePoint(s string) point {
	triple := strings.SplitN(s, " ", 3)
	if len(triple) < 3 {
		fatal("failed parsing input string: %s", triple)
	}
	thisTs, err := strconv.ParseInt(triple[2], 10, 64)
	if err != nil {
		fatal("failed parsing the timestamp: %s", thisTs)
	}
	thisVal, _ := strconv.ParseFloat(triple[1], 64)
	if err != nil {
		fatal("failed parsing the value: %s", thisVal)
	}
	el := &data.Float64Element{
		Timestamp: thisTs,
		Value:     thisVal,
	}
	return point{
		sensor:  triple[0],
		element: *el,
	}
}
