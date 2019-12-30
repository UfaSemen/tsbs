package main

//go:generate msgp

import (
	"bufio"
	"bytes"
	"github.com/timescale/tsbs/load"
	"strconv"
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
	element []byte
}

func (f *factory) New() load.Batch {
	return &batch{
		m:   map[string]batchData{},
		cnt: 0,
	}
}

type batch struct {
	m   map[string]batchData
	cnt int
}

func (b *batch) Len() int {
	return b.cnt
}

func (b *batch) Append(item *load.Point) {
	this := item.Data.(*point)
	s := this.sensor
	el := b.m[s]
	el.buf.Write(this.element)
	//el.buf.Write([]byte("\n"))
	el.len++
	b.m[s] = el
	b.cnt++
}

type batchData struct {
	buf bytes.Buffer
	len int
}

type decoder struct {
	scanner    *bufio.Scanner
	senNum     int
	batchSize  int
	sensor     int
	numInBatch int
	n          int
	end        int
	c          int
	//readStrs   []string
}

func (d *decoder) Decode(bf *bufio.Reader) *load.Point {
	//var p point
	data, err := bf.ReadBytes('\n')
	if err != nil {
		return nil
	}
	sen := strconv.Itoa(d.n)
	p := point{
		sensor:  "sensor_" + sen,
		element: data,
	}
	d.n++
	if d.n == d.senNum {
		d.n = 0
	}

	//if d.sensor == 0 {
	//	for i := 0; i < senNum; i++ {
	//		ok := d.scanner.Scan()
	//		if !ok && d.scanner.Err() == nil {
	//			if d.numInBatch == 0 {
	//				return nil
	//			}
	//			d.end = d.numInBatch
	//			d.sensor = (d.sensor + 1) % d.senNum
	//			d.numInBatch = 0
	//			d.n = 0
	//			break
	//		} else if !ok {
	//			fatal("scan error: %v", d.scanner.Err())
	//			return nil
	//		}
	//		//d.readStrs[d.n] = d.scanner.Text()
	//		d.n++
	//	}
	//}
	////p = d.parsePoint(d.readStrs[d.numInBatch*d.senNum+d.sensor])
	//d.numInBatch++
	//if d.numInBatch == d.batchSize || d.numInBatch == d.end {
	//	d.sensor = (d.sensor + 1) % d.senNum
	//	d.numInBatch = 0
	//	if d.sensor == 0 {
	//		d.n = 0
	//	}
	//}
	return load.NewPoint(&p)
}

//func (d *decoder) parsePoint(s string) point {
//	triple := strings.SplitN(s, " ", 3)
//	if len(triple) < 3 {
//		fatal("failed parsing input string: %s", triple)
//	}
//	thisTs, err := strconv.ParseInt(triple[2], 10, 64)
//	if err != nil {
//		fatal("failed parsing the timestamp: %s", thisTs)
//	}
//	thisVal, _ := strconv.ParseFloat(triple[1], 64)
//	if err != nil {
//		fatal("failed parsing the value: %s", thisVal)
//	}
//	el := &data.Float64Element{
//		Timestamp: thisTs,
//		Value:     thisVal,
//	}
//	return point{
//		sensor:  triple[0],
//		element: *el,
//	}
//}
