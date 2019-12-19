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
	scanner *bufio.Scanner
}

func (d *decoder) Decode(bf *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil {
		return nil // EOF
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}

	triple := strings.SplitN(d.scanner.Text(), " ", 3)
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
	return load.NewPoint(&point{
		sensor:  triple[0],
		element: *el,
	})
}
