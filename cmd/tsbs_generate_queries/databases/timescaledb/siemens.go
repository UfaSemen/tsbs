package timescaledb

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
	sql := fmt.Sprintf(`SELECT * FROM %s
		WHERE time >= '%s' AND time < '%s'
		ORDER BY time`,
		sensor,
		in.Start().Format(goTimeFmt),
		in.End().Format(goTimeFmt),
	)

	humanLabel := fmt.Sprintf("TimescaleDB raw data, random %s sensor, random %s time range", sensor, interval)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, in.StartString())
	s.fillInQuery(q, humanLabel, humanDesc, sensor, sql)
}

//Search queries all hour long periods which have some outlier values for random time period of given size of random sensor
func (s *Siemens) Search(q query.Query, interval time.Duration) {
	in := s.Interval.MustRandWindow(interval)
	st := s.GetRandomSearchTable()
	sql := fmt.Sprintf(`SELECT hour FROM %s
		WHERE hour >= '%s' AND hour < '%s' AND (min <= %d OR max >= %d)
		ORDER BY hour`,
		st,
		in.Start().Format(goTimeFmt),
		in.End().Format(goTimeFmt),
		siemens.MinSearchLimit,
		siemens.MaxSearchLimit,
	)

	humanLabel := fmt.Sprintf("TimescaleDB search, random %s search table, random %s time range", st, interval)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, in.StartString())
	s.fillInQuery(q, humanLabel, humanDesc, st, sql)
}

//SampledData queries data sampled with given resolution for random time period of given size of random sensor
func (s *Siemens) SampledData(q query.Query, interval, resolution time.Duration) {
	in := s.Interval.MustRandWindow(interval)
	sensor := s.GetRandomSensor()
	sql := fmt.Sprintf(`SELECT first(time, time) AS sampled_time, first(value, time) AS sampled_value FROM %s
		WHERE time >= '%s' AND time < '%s'
		GROUP BY %s
		ORDER BY sampled_time`,
		sensor,
		in.Start().Format(goTimeFmt),
		in.End().Format(goTimeFmt),
		fmt.Sprintf(timeBucketFmt, int(resolution.Seconds())),
	)

	humanLabel := fmt.Sprintf("TimescaleDB sampled data, random %s sensor, random %s time range", sensor, interval)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, in.StartString())
	s.fillInQuery(q, humanLabel, humanDesc, sensor, sql)
}

//Maximum queries maximum values of data with given resolution for random time period of given size of random sensor
func (s *Siemens) Maximum(q query.Query, interval, resolution time.Duration) {
	in := s.Interval.MustRandWindow(interval)
	sensor := s.GetRandomSensor()
	sql := fmt.Sprintf(`SELECT %s AS resolution, MAX(value) FROM %s
		WHERE time >= '%s' AND time < '%s'
		GROUP BY resolution
		ORDER BY resolution`,
		fmt.Sprintf(timeBucketFmt, int(resolution.Seconds())),
		sensor,
		in.Start().Format(goTimeFmt),
		in.End().Format(goTimeFmt),
	)

	humanLabel := fmt.Sprintf("TimescaleDB raw, random %s sensor, random %s time range", sensor, interval)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, in.StartString())
	s.fillInQuery(q, humanLabel, humanDesc, sensor, sql)
}

//Difference queries differences with last value for each row for random time period of given size of random sensor
func (s *Siemens) Difference(q query.Query, interval time.Duration) {
	in := s.Interval.MustRandWindow(interval)
	sensor := s.GetRandomSensor()
	sql := fmt.Sprintf(`SELECT time, value - LAG(value) OVER() AS difference FROM %s
		WHERE time >= '%s' AND time < '%s'`,
		sensor,
		in.Start().Format(goTimeFmt),
		in.End().Format(goTimeFmt),
	)

	humanLabel := fmt.Sprintf("TimescaleDB raw data, random %s sensor, random %s time range", sensor, interval)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, in.StartString())
	s.fillInQuery(q, humanLabel, humanDesc, sensor, sql)
}
