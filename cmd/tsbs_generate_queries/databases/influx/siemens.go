package influx

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/siemens"
	"github.com/timescale/tsbs/query"
	"time"
)

// Siemens produces TimescaleDB-specific queries for all the siemens query types.
type Siemens struct {
	*BaseGenerator
	*siemens.Core
}

func (s *Siemens) RawData(qi query.Query, d time.Duration) {
	interval := s.Interval.MustRandWindow(d)
	sensor := s.GetRandomSensor()

	influxql := fmt.Sprintf(`SELECT "value" FROM "%s" WHERE time > '%s' AND time <= '%s'`,
		sensor,
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
	)

	humanLabel := fmt.Sprintf("InfluxDB all values for random %s from sensor", d.String())
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, sensor)

	s.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (s *Siemens) SampledData(qi query.Query, d, gd time.Duration) {
	interval := s.Interval.MustRandWindow(d)
	sensor := s.GetRandomSensor()

	influxql := fmt.Sprintf(`SELECT first("value") FROM "%s" WHERE time > '%s' AND time <= '%s' GROUP BY time(%s)`,
		sensor,
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		gd.String(),
	)

	humanLabel := fmt.Sprintf("InfluxDB sample for random %s over %s intervals", d.String(), gd.String())
	humanDesc := fmt.Sprintf("%s from sensor: %s", humanLabel, sensor)

	s.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (s *Siemens) Maximum(qi query.Query, d, gd time.Duration) {
	interval := s.Interval.MustRandWindow(d)
	sensor := s.GetRandomSensor()

	influxql := fmt.Sprintf(`SELECT max("value") FROM "%s" WHERE time > '%s' AND time <= '%s' GROUP BY time(%s)`,
		sensor,
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		gd.String(),
	)

	humanLabel := fmt.Sprintf("InfluxDB maximum value for random %s over %s intervals", d.String(), gd.String())
	humanDesc := fmt.Sprintf("%s from sensor: %s", humanLabel, sensor)

	s.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (s *Siemens) Difference(qi query.Query, d time.Duration) {
	interval := s.Interval.MustRandWindow(d)
	sensor := s.GetRandomSensor()

	influxql := fmt.Sprintf(`SELECT difference("value") FROM "%s" WHERE time > '%s' AND time <= '%s'`,
		sensor,
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
	)

	humanLabel := fmt.Sprintf("InfluxDB difference between values for random %s", d.String())
	humanDesc := fmt.Sprintf("%s from sensor: %s", humanLabel, sensor)

	s.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (s *Siemens) Search(qi query.Query, d time.Duration) {
	interval := s.Interval.MustRandWindow(d)
	sensor := s.GetRandomSensor()
	influxql := fmt.Sprintf(`SELECT time,min_value FROM benchmark_search.autogen.%s WHERE (min_value <= %d OR max_value >= %d) AND (time > '%s' AND time <= '%s')`,
		sensor,
		siemens.MinSearchLimit,
		siemens.MaxSearchLimit,
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
	)

	humanLabel := fmt.Sprintf("InfluxDB search, random %s time range", d)
	humanDesc := fmt.Sprintf("%s, random %s search table: %s, start interval: %s", humanLabel, sensor, interval.StartString())

	s.fillInQuery(qi, humanLabel, humanDesc, influxql)
}
