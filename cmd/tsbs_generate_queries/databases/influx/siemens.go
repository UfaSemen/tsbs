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

	influxql := fmt.Sprintf(`SELECT "value" FROM "%s" WHERE time > '%s' AND time <= '%s'`,
		s.GetRandomSensor(),
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
	)

	humanLabel := "Influx all value from sensor"
	humanDesc := humanLabel

	s.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (s *Siemens) SampledData(qi query.Query, d, gd time.Duration) {
	interval := s.Interval.MustRandWindow(d)

	influxql := fmt.Sprintf(`SELECT last("value") FROM "%s" WHERE time > '%s' AND time <= '%s' GROUP BY time(%s)`,
		s.GetRandomSensor(),
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		gd.String(),
	)

	humanLabel := "Influx sample value"
	humanDesc := humanLabel

	s.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (s *Siemens) Maximum(qi query.Query, d, gd time.Duration) {
	interval := s.Interval.MustRandWindow(d)

	influxql := fmt.Sprintf(`SELECT max("value") FROM "%s" WHERE time > '%s' AND time <= '%s' GROUP BY time(%s)`,
		s.GetRandomSensor(),
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
		gd.String(),
	)

	humanLabel := "Influx maximum value"
	humanDesc := humanLabel

	s.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (s *Siemens) Difference(qi query.Query, d time.Duration) {
	interval := s.Interval.MustRandWindow(d)

	influxql := fmt.Sprintf(`SELECT difference("value") FROM "%s" WHERE time > '%s' AND time <= '%s'`,
		s.GetRandomSensor(),
		interval.Start().Format(time.RFC3339),
		interval.End().Format(time.RFC3339),
	)

	humanLabel := "Influx difference between values"
	humanDesc := humanLabel

	s.fillInQuery(qi, humanLabel, humanDesc, influxql)
}
