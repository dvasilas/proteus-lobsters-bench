package benchmark

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	"github.com/dvasilas/proteus-lobsters-bench/internal/perf"
	"github.com/dvasilas/proteus-lobsters-bench/internal/workload"
	log "github.com/sirupsen/logrus"
)

// Benchmark ...
type Benchmark struct {
	config       *config.BenchmarkConfig
	workload     *workload.Workload
	measurements *perf.Perf
}

// NewBenchmark ...
func NewBenchmark(configFile string, preload bool, threadCnt int, dryRun bool) (Benchmark, error) {
	rand.Seed(time.Now().UnixNano())

	conf, err := config.GetConfig(configFile)
	if err != nil {
		return Benchmark{}, err
	}
	conf.Benchmark.DoPreload = preload
	if threadCnt > 0 {
		conf.Benchmark.ThreadCount = threadCnt
	}

	log.WithFields(log.Fields{"conf": conf}).Info("configuration")

	if dryRun {
		conf.Print()
		return Benchmark{}, nil
	}

	workload, err := workload.NewWorkload(&conf)
	if err != nil {
		return Benchmark{}, err
	}

	return Benchmark{
		config:       &conf,
		workload:     workload,
		measurements: perf.New(),
	}, nil
}

// Preload ...
func (b Benchmark) Preload() error {
	return b.workload.Preload()
}

// Run ...
func (b Benchmark) Run() error {
	var wg sync.WaitGroup

	for i := 0; i < b.config.Benchmark.ThreadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			measurements, measurementBufferSize, startTime, endTime := b.workload.Run(b.config.Benchmark.OpCount)
			b.measurements.ReportMeasurements(measurements, measurementBufferSize, startTime, endTime)
		}()
	}

	wg.Wait()

	b.workload.Close()

	return nil
}

// PrintMeasurements ...
func (b Benchmark) PrintMeasurements() {
	b.config.Print()

	metrics := b.measurements.CalculateMetrics()

	fmt.Printf("Runtime(s): %.3f\n", metrics.Runtime)
	for opType, metrics := range metrics.PerOpMetrics {
		fmt.Printf("[%s] Operation count: %d\n", opType, metrics.OpCount)
		fmt.Printf("[%s] Throughput: %.5f\n", opType, metrics.Throughput)
		fmt.Printf("[%s] p50(ms): %.5f\n", opType, metrics.P50)
		fmt.Printf("[%s] p90(ms): %.5f\n", opType, metrics.P90)
		fmt.Printf("[%s] p95(ms): %.5f\n", opType, metrics.P95)
		fmt.Printf("[%s] p99(ms): %.5f\n", opType, metrics.P99)
	}
}
