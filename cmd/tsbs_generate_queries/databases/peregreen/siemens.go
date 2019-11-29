package peregreen

import (
	"fmt"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/siemens"
	"github.com/timescale/tsbs/query"
)

// Siemens produces TimescaleDB-specific queries for all the siemens query types.
type Siemens struct {
	*BaseGenerator
	*siemens.Core
}

//RawData queries all raw data for random time period of given size of random sensor
func (s *Siemens) RawData(q query.Query, interval time.Duration) {
	in := s.Interval.MustRandWindow(interval)
	sensor := s.GetRandomSensor()
	path := fmt.Sprintf(`/extract/%s/%d-%d/0/none/msgp`,
		sensor,
		in.Start().Unix()*1000,
		in.End().Unix()*1000,
	)

	humanLabel := fmt.Sprintf("Peregreen raw data, random %s sensor, random %s time range", sensor, interval)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, in.StartString())
	s.fillInQuery(q, humanLabel, humanDesc, "GET", "", path)
}

//Search queries all hour long periods which have some outlier values for random time period of given size of random sensor
func (s *Siemens) Search(q query.Query, interval time.Duration) {
	in := s.Interval.MustRandWindow(interval)
	sensor := s.GetRandomSensor()
	path := fmt.Sprintf(`/search/%s/%d-%d/msgp`,
		sensor,
		in.Start().Unix()*1000,
		in.End().Unix()*1000,
	)
	body := fmt.Sprintf(`min lt %d | max gt %d`,
		siemens.MinSearchLimit,
		siemens.MaxSearchLimit,
	)

	humanLabel := fmt.Sprintf("Peregreen search, random %s table, random %s time range", sensor, interval)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, in.StartString())
	s.fillInQuery(q, humanLabel, humanDesc, "POST", body, path)
}

//SampledData queries data sampled with given resolution for random time period of given size of random sensor
func (s *Siemens) SampledData(q query.Query, interval, resolution time.Duration) {
	in := s.Interval.MustRandWindow(interval)
	sensor := s.GetRandomSensor()
	path := fmt.Sprintf(`/extract/%s/%d-%d/%s/none/msgp`,
		sensor,
		in.Start().Unix()*1000,
		in.End().Unix()*1000,
		resolution.String(),
	)

	humanLabel := fmt.Sprintf("Peregreen sampled data, random %s sensor, random %s time range", sensor, interval)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, in.StartString())
	s.fillInQuery(q, humanLabel, humanDesc, "GET", "", path)
}

//Maximum queries maximum values of data with given resolution for random time period of given size of random sensor
func (s *Siemens) Maximum(q query.Query, interval, resolution time.Duration) {
	in := s.Interval.MustRandWindow(interval)
	sensor := s.GetRandomSensor()
	path := fmt.Sprintf(`/extract/%s/%d-%d/%s/max/msgp`,
		sensor,
		in.Start().Unix()*1000,
		in.End().Unix()*1000,
		resolution.String(),
	)

	humanLabel := fmt.Sprintf("Peregreen maximum, random %s sensor, random %s time range", sensor, interval)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, in.StartString())
	s.fillInQuery(q, humanLabel, humanDesc, "GET", "", path)
}

//Difference queries differences with last value for each row for random time period of given size of random sensor
func (s *Siemens) Difference(q query.Query, interval time.Duration) {
	in := s.Interval.MustRandWindow(interval)
	sensor := s.GetRandomSensor()
	path := fmt.Sprintf(`/extract/%s/%d-%d/0/transform:diff/msgp`,
		sensor,
		in.Start().Unix()*1000,
		in.End().Unix()*1000,
	)

	humanLabel := fmt.Sprintf("Peregreen difference, random %s sensor, random %s time range", sensor, interval)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, in.StartString())
	s.fillInQuery(q, humanLabel, humanDesc, "GET", "", path)
}
