package main

//go:generate msgp

import (
	"bufio"
	"strconv"
	"strings"

	"github.com/timescale/tsbs/load"
	"github.com/visheratin/tss/data"
)

type factory struct{}

type point struct {
	sensor  string
	element data.Float64Element
}

func (f *factory) New() load.Batch {
	return &batch{}
}

type batch struct {
	m   map[string]data.Float64Elements
	cnt int
}

func (b batch) Len() int {
	return b.cnt
}

func (b batch) Append(item *load.Point) {
	this := item.Data.(point)

	b.m[this.sensor] = append(b.m[this.sensor], this.element)
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
	thisTs, _ := strconv.ParseInt(triple[1], 64, 10)
	thisVal, _ := strconv.ParseFloat(triple[2], 64)
	el := &data.Float64Element{
		Timestamp: thisTs,
		Value:     thisVal,
	}
	return load.NewPoint(&point{
		sensor:  triple[0],
		element: *el,
	})
}
