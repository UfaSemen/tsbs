package clickhouse

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
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

	sql := fmt.Sprintf(`SELECT time, value FROM %s WHERE (created_at >= '%s') AND (created_at < '%s')`,
		sensor,
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat))

	humanLabel := fmt.Sprintf("Clickhouse all values for random %s from sensor", d.String())
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, sensor)
	s.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (s *Siemens) SampledData(qi query.Query, d, gd time.Duration) {
	interval := s.Interval.MustRandWindow(d)
	sensor := s.GetRandomSensor()

	sqlFormat := `
		SELECT
       		%s(created_at) as timeGroup, any(value)
		FROM %s 
		WHERE (created_at >= '%s') AND (created_at < '%s') 
		GROUP BY timeGroup
		ORDER BY timeGroup
	`
	humanLabel := fmt.Sprintf("Clickhouse sample for random %s over %s intervals", d.String(), gd.String())
	humanDesc := fmt.Sprintf("%s from sensor: %s", humanLabel, sensor)

	switch gd {
	case time.Hour:
		sql := fmt.Sprintf(sqlFormat,
			"toStartOfHour",
			sensor,
			interval.Start().Format(clickhouseTimeStringFormat),
			interval.End().Format(clickhouseTimeStringFormat))

		s.fillInQuery(qi, humanLabel, humanDesc, sensor, sql)
		return
	case 24 * time.Hour:
		sql := fmt.Sprintf(sqlFormat,
			"toStartOfDay",
			sensor,
			interval.Start().Format(clickhouseTimeStringFormat),
			interval.End().Format(clickhouseTimeStringFormat))
		s.fillInQuery(qi, humanLabel, humanDesc, sensor, sql)
		return
	case 7 * 24 * time.Hour:
		sql := fmt.Sprintf(sqlFormat,
			"toStartOfWeek",
			sensor,
			interval.Start().Format(clickhouseTimeStringFormat),
			interval.End().Format(clickhouseTimeStringFormat))
		s.fillInQuery(qi, humanLabel, humanDesc, sensor, sql)
		return
	default:
		panic(fmt.Errorf("Sample interval not found"))
	}
}

func (s *Siemens) Maximum(qi query.Query, d, gd time.Duration) {
	interval := s.Interval.MustRandWindow(d)
	sensor := s.GetRandomSensor()

	sqlFormat := `
		SELECT
       		%s(created_at) as timeGroup, max(value)
		FROM %s 
		WHERE (created_at >= '%s') AND (created_at < '%s') 
		GROUP BY timeGroup
		ORDER BY timeGroup
	`
	humanLabel := fmt.Sprintf("Clickhouse maximum value for random %s over %s intervals", d.String(), gd.String())
	humanDesc := fmt.Sprintf("%s from sensor: %s", humanLabel, sensor)

	switch gd {
	case time.Hour:
		sql := fmt.Sprintf(sqlFormat,
			"toStartOfHour",
			sensor,
			interval.Start().Format(clickhouseTimeStringFormat),
			interval.End().Format(clickhouseTimeStringFormat))

		s.fillInQuery(qi, humanLabel, humanDesc, sensor, sql)
		return
	case 24 * time.Hour:
		sql := fmt.Sprintf(sqlFormat,
			"toStartOfDay",
			sensor,
			interval.Start().Format(clickhouseTimeStringFormat),
			interval.End().Format(clickhouseTimeStringFormat))
		s.fillInQuery(qi, humanLabel, humanDesc, sensor, sql)
		return
	case 7 * 24 * time.Hour:
		sql := fmt.Sprintf(sqlFormat,
			"toStartOfWeek",
			sensor,
			interval.Start().Format(clickhouseTimeStringFormat),
			interval.End().Format(clickhouseTimeStringFormat))
		s.fillInQuery(qi, humanLabel, humanDesc, sensor, sql)
		return
	default:
		panic(fmt.Errorf("Sample interval not found"))
	}
}

func (s *Siemens) Difference(qi query.Query, d time.Duration) {
	interval := s.Interval.MustRandWindow(d)
	sensor := s.GetRandomSensor()

	sql := fmt.Sprintf(`SELECT time, runningDifference(value) FROM %s WHERE (created_at >= '%s') AND (created_at < '%s')`,
		sensor,
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat))

	humanLabel := fmt.Sprintf("Clickhouse difference between values for random %s", d.String())
	humanDesc := fmt.Sprintf("%s from sensor: %s", humanLabel, sensor)
	s.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (s *Siemens) Search(qi query.Query, d time.Duration) {
	interval := s.Interval.MustRandWindow(d)
	table := s.GetRandomSearchTable()
	sql := fmt.Sprintf(`SELECT hour FROM %s WHERE (min <= %d OR max >= %d) AND (time >= '%s' AND time < '%s')`,
		table,
		siemens.MinSearchLimit,
		siemens.MaxSearchLimit,
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat),
	)

	humanLabel := fmt.Sprintf("Clickhouse search, random %s search table, random %s time range", table, d)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	s.fillInQuery(qi, humanLabel, humanDesc, table, sql)
}
