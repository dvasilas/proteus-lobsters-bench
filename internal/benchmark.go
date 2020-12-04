package benchmark

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	"github.com/dvasilas/proteus-lobsters-bench/internal/generator"
	getmetrics "github.com/dvasilas/proteus-lobsters-bench/internal/getMetrics"
	"github.com/dvasilas/proteus-lobsters-bench/internal/measurements"
	log "github.com/sirupsen/logrus"
)

// Benchmark ...
type Benchmark struct {
	config       *config.BenchmarkConfig
	generator    *generator.Generator
	measurements *measurements.Measurements
}

// NewBenchmark ...
func NewBenchmark(configFile string, preload bool, threadCnt int, load, maxInFlightR, maxInFlightW int64, dryRun bool, fM *os.File) (Benchmark, error) {
	rand.Seed(time.Now().UnixNano())

	conf, err := config.GetConfig(configFile)
	if err != nil {
		return Benchmark{}, err
	}
	conf.Benchmark.DoPreload = preload
	if threadCnt > 0 {
		conf.Benchmark.ThreadCount = threadCnt
	}

	if load > 0 {
		conf.Benchmark.TargetLoad = load / int64(threadCnt)
	}

	if maxInFlightR > 0 {
		conf.Benchmark.MaxInFlightRead = maxInFlightR
	}

	if maxInFlightW > 0 {
		conf.Benchmark.MaxInFlightWrite = maxInFlightW
	}

	log.WithFields(log.Fields{"conf": conf}).Info("configuration")

	if dryRun {
		if err := conf.Print(fM); err != nil {
			return Benchmark{}, err
		}
		return Benchmark{}, nil
	}

	generator, err := generator.NewGenerator(&conf)
	if err != nil {
		return Benchmark{}, err
	}

	return Benchmark{
		config:       &conf,
		generator:    generator,
		measurements: measurements.New(),
	}, nil
}

// Run ...
func (b Benchmark) Run() error {
	var wg sync.WaitGroup

	for i := 0; i < b.config.Benchmark.ThreadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clientMeasurements := b.generator.Client()
			b.measurements.ReportMeasurements(clientMeasurements)
		}()
	}

	wg.Wait()

	b.generator.Close()

	return nil
}

// Preload ...
func (b Benchmark) Preload() error {
	return b.generator.Preload()
}

// Test ...
func (b Benchmark) Test() error {
	return b.generator.Test()
}

// PrintMeasurements ...
func (b Benchmark) PrintMeasurements(fM, fTRead, fTWrite *os.File) error {
	if err := b.config.Print(fM); err != nil {
		return err
	}

	metrics, err := b.measurements.CalculateMetrics(fTRead, fTWrite)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(fM, "Runtime(s): %.3f\n", metrics.Runtime.Seconds()); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(fM, "Load offered: %.3f\n", metrics.LoadOffered); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(fM, "Total throughput: %.5f\n", metrics.Throughput); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(fM, "Aborted ops: %d\n", metrics.DeadlockAborts); err != nil {
		return err
	}
	for opType, metrics := range metrics.PerOpMetrics {
		if _, err := fmt.Fprintf(fM, "[%s] Operation count: %d\n", opType, metrics.OpCount); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[%s] Throughput: %.5f\n", opType, metrics.Throughput); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[%s] p50(ms): %.5f\n", opType, metrics.P50); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[%s] p90(ms): %.5f\n", opType, metrics.P90); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[%s] p95(ms): %.5f\n", opType, metrics.P95); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[%s] p99(ms): %.5f\n", opType, metrics.P99); err != nil {
			return err
		}
	}

	getmetrics.GetMetrics(*b.config, fM)

	return nil
}
