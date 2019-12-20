package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
)

// Global vars
var loader *load.BenchmarkRunner

// for comfy testing
var fatal = log.Fatalf

// Params for batch processing have to be initialized inside init()
var (
	host      string
	port      string
	senNum    int
	batchSize int
	debugPort int
)

// Parse args:
func init() {
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("host", "localhost", "Hostname of Peregreen instance")
	pflag.String("port", "47375", "Which port to connect to on the database host")
	pflag.Uint("sensors", 100, "Number of sensors in dataset")
	pflag.Uint("debug-port", 0, "Debug port for prometheus and pprof")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	host = viper.GetString("host")
	port = viper.GetString("port")
	senNum = viper.GetInt("sensors")
	batchSize = viper.GetInt("batch-size")
	debugPort = viper.GetInt("debug-port")

	if debugPort > 0 {
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			log.Println(http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", debugPort), nil))
		}()
	}

	loader = load.GetBenchmarkRunner(config)
}

type benchmark struct{}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{
		scanner:   bufio.NewScanner(br),
		senNum:    senNum,
		batchSize: batchSize,
		readStrs:  make([]string, batchSize*senNum),
	}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	return &sensorIndexer{partitions: maxPartitions}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return &dbCreator{}
}

func main() {
	loader.RunBenchmark(&benchmark{}, load.WorkerPerQueue)
}
