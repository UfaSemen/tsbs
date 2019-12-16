// tsbs_generate_queries generates queries for various use cases. Its output will
// be consumed by the corresponding tsbs_run_queries_ program.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/siemens"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/internal/inputs"
	internalutils "github.com/timescale/tsbs/internal/utils"
)

var useCaseMatrix = map[string]map[string]utils.QueryFillerMaker{
	"devops": {
		devops.LabelSingleGroupby + "-1-1-1":  devops.NewSingleGroupby(1, 1, 1),
		devops.LabelSingleGroupby + "-1-1-12": devops.NewSingleGroupby(1, 1, 12),
		devops.LabelSingleGroupby + "-1-8-1":  devops.NewSingleGroupby(1, 8, 1),
		devops.LabelSingleGroupby + "-5-1-1":  devops.NewSingleGroupby(5, 1, 1),
		devops.LabelSingleGroupby + "-5-1-12": devops.NewSingleGroupby(5, 1, 12),
		devops.LabelSingleGroupby + "-5-8-1":  devops.NewSingleGroupby(5, 8, 1),
		devops.LabelMaxAll + "-1":             devops.NewMaxAllCPU(1),
		devops.LabelMaxAll + "-8":             devops.NewMaxAllCPU(8),
		devops.LabelDoubleGroupby + "-1":      devops.NewGroupBy(1),
		devops.LabelDoubleGroupby + "-5":      devops.NewGroupBy(5),
		devops.LabelDoubleGroupby + "-all":    devops.NewGroupBy(devops.GetCPUMetricsLen()),
		devops.LabelGroupbyOrderbyLimit:       devops.NewGroupByOrderByLimit,
		devops.LabelHighCPU + "-all":          devops.NewHighCPU(0),
		devops.LabelHighCPU + "-1":            devops.NewHighCPU(1),
		devops.LabelLastpoint:                 devops.NewLastPointPerHost,
	},
	"iot": {
		iot.LabelLastLoc:                       iot.NewLastLocPerTruck,
		iot.LabelLastLocSingleTruck:            iot.NewLastLocSingleTruck,
		iot.LabelLowFuel:                       iot.NewTruckWithLowFuel,
		iot.LabelHighLoad:                      iot.NewTruckWithHighLoad,
		iot.LabelStationaryTrucks:              iot.NewStationaryTrucks,
		iot.LabelLongDrivingSessions:           iot.NewTrucksWithLongDrivingSession,
		iot.LabelLongDailySessions:             iot.NewTruckWithLongDailySession,
		iot.LabelAvgVsProjectedFuelConsumption: iot.NewAvgVsProjectedFuelConsumption,
		iot.LabelAvgDailyDrivingDuration:       iot.NewAvgDailyDrivingDuration,
		iot.LabelAvgDailyDrivingSession:        iot.NewAvgDailyDrivingSession,
		iot.LabelAvgLoad:                       iot.NewAvgLoad,
		iot.LabelDailyActivity:                 iot.NewDailyTruckActivity,
		iot.LabelBreakdownFrequency:            iot.NewTruckBreakdownFrequency,
	},
	"siemens": {
		siemens.LabelRawData + siemens.LabelDay:                           siemens.NewRawData(24 * time.Hour),
		siemens.LabelRawData + siemens.LabelMonth:                         siemens.NewRawData(30 * 24 * time.Hour),
		siemens.LabelRawData + siemens.LabelYear:                          siemens.NewRawData(365 * 24 * time.Hour),
		siemens.LabelRawData + siemens.LabelHour:                          siemens.NewRawData(time.Hour),
		siemens.LabelRawData + siemens.LabelWeek:                          siemens.NewRawData(7 * time.Hour),
		siemens.LabelSearch + siemens.LabelDay:                            siemens.NewSearch(24 * time.Hour),
		siemens.LabelSearch + siemens.LabelMonth:                          siemens.NewSearch(30 * 24 * time.Hour),
		siemens.LabelSearch + siemens.LabelYear:                           siemens.NewSearch(365 * 24 * time.Hour),
		siemens.LabelSearch + siemens.LabelHour:                           siemens.NewSearch(365 * 24 * time.Hour),
		siemens.LabelSearch + siemens.LabelWeek:                           siemens.NewSearch(365 * 24 * time.Hour),
		siemens.LabelSampledData + siemens.LabelDay + siemens.LabelHour:   siemens.NewSampledData(24*time.Hour, time.Hour),
		siemens.LabelSampledData + siemens.LabelDay + siemens.LabelDay:    siemens.NewSampledData(24*time.Hour, 24*time.Hour),
		siemens.LabelSampledData + siemens.LabelMonth + siemens.LabelHour: siemens.NewSampledData(30*24*time.Hour, time.Hour),
		siemens.LabelSampledData + siemens.LabelMonth + siemens.LabelDay:  siemens.NewSampledData(30*24*time.Hour, 24*time.Hour),
		siemens.LabelSampledData + siemens.LabelMonth + siemens.LabelWeek: siemens.NewSampledData(30*24*time.Hour, 7*24*time.Hour),
		siemens.LabelSampledData + siemens.LabelYear + siemens.LabelHour:  siemens.NewSampledData(365*24*time.Hour, time.Hour),
		siemens.LabelSampledData + siemens.LabelYear + siemens.LabelDay:   siemens.NewSampledData(365*24*time.Hour, 24*time.Hour),
		siemens.LabelSampledData + siemens.LabelYear + siemens.LabelWeek:  siemens.NewSampledData(365*24*time.Hour, 7*24*time.Hour),
		siemens.LabelMaximum + siemens.LabelDay + siemens.LabelHour:       siemens.NewMaximum(24*time.Hour, time.Hour),
		siemens.LabelMaximum + siemens.LabelDay + siemens.LabelDay:        siemens.NewMaximum(24*time.Hour, 24*time.Hour),
		siemens.LabelMaximum + siemens.LabelMonth + siemens.LabelHour:     siemens.NewMaximum(30*24*time.Hour, time.Hour),
		siemens.LabelMaximum + siemens.LabelMonth + siemens.LabelDay:      siemens.NewMaximum(30*24*time.Hour, 24*time.Hour),
		siemens.LabelMaximum + siemens.LabelMonth + siemens.LabelWeek:     siemens.NewMaximum(30*24*time.Hour, 7*24*time.Hour),
		siemens.LabelMaximum + siemens.LabelYear + siemens.LabelHour:      siemens.NewMaximum(365*24*time.Hour, time.Hour),
		siemens.LabelMaximum + siemens.LabelYear + siemens.LabelDay:       siemens.NewMaximum(365*24*time.Hour, 24*time.Hour),
		siemens.LabelMaximum + siemens.LabelYear + siemens.LabelWeek:      siemens.NewMaximum(365*24*time.Hour, 7*24*time.Hour),
		siemens.LabelDifference + siemens.LabelDay:                        siemens.NewDifference(24 * time.Hour),
		siemens.LabelDifference + siemens.LabelMonth:                      siemens.NewDifference(30 * 24 * time.Hour),
		siemens.LabelDifference + siemens.LabelYear:                       siemens.NewDifference(365 * 24 * time.Hour),
		siemens.LabelDifference + siemens.LabelHour:                       siemens.NewDifference(time.Hour),
		siemens.LabelDifference + siemens.LabelWeek:                       siemens.NewDifference(7 * time.Hour),
	},
}

var config = &inputs.QueryGeneratorConfig{}

// Parse args:
func init() {
	useCaseMatrix["cpu-only"] = useCaseMatrix["devops"]
	// Change the Usage function to print the use case matrix of choices:
	oldUsage := pflag.Usage
	pflag.Usage = func() {
		oldUsage()

		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "The use case matrix of choices is:\n")
		for uc, queryTypes := range useCaseMatrix {
			for qt := range queryTypes {
				fmt.Fprintf(os.Stderr, "  use case: %s, query type: %s\n", uc, qt)
			}
		}
	}

	config.AddToFlagSet(pflag.CommandLine)

	pflag.Parse()

	err := internalutils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config.BaseConfig); err != nil {
		panic(fmt.Errorf("unable to decode base config: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
}

func main() {
	qg := inputs.NewQueryGenerator(useCaseMatrix)
	err := qg.Generate(config)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}
