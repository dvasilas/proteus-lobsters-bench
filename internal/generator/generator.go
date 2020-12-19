package generator

import (
	"math/rand"
	"sync"
	"time"

	"fmt"
	//"sync/atomic"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	"github.com/dvasilas/proteus-lobsters-bench/internal/measurements"
	"github.com/dvasilas/proteus-lobsters-bench/internal/operations"
	"github.com/dvasilas/proteus-lobsters-bench/internal/workload"
	"google.golang.org/grpc/benchmark/stats"
)

// Generator ...
type Generator struct {
	config   *config.BenchmarkConfig
	workload *workload.Workload
}

// NewGenerator ...
func NewGenerator(conf *config.BenchmarkConfig) (*Generator, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	workload, err := workload.NewWorkload(conf)
	if err != nil {
		return nil, err
	}

	return &Generator{
		workload: workload,
		config:   conf,
	}, nil
}

func calculateOpGenerationRate(targetLoad int64) time.Duration {
	return time.Duration(1e9/float64(targetLoad)) * time.Nanosecond
}

// Client ...
func (g *Generator) Client() measurements.ClientMeasurements {

	// perform a new operation every interArrival
	interArrival := calculateOpGenerationRate(g.config.Benchmark.TargetLoad)

	// each operation is responsible for measuring its latency
	// measurementsCh is used to gather latency measurements
	measurementsCh := make(chan measurements.Measurement)

	var opCnt, deadlockAborts, opID int64
	var op operations.Operation
	var st, now, next time.Time

	st = time.Now()
	end := st.Add(time.Duration(g.config.Benchmark.Runtime) * time.Second)
	warmpupEnd := st.Add(time.Duration(g.config.Benchmark.Warmup) * time.Second)

	limitReadCh := make(chan struct{}, g.config.Benchmark.MaxInFlightRead)
	limitWriteCh := make(chan struct{}, g.config.Benchmark.MaxInFlightWrite)
	//	var inFlightR, maxInFlightR int64
	limitThreads := true
	if g.config.Benchmark.MaxInFlightWrite == 1 && g.config.Benchmark.MaxInFlightRead == 1 {
		limitThreads = false
	}

	histograms := make(map[string]*stats.Histogram)
	histograms["read"] = measurements.NewHistogram()
	histograms["write"] = measurements.NewHistogram()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		measurementsConsumer(measurementsCh, histograms, &deadlockAborts, warmpupEnd, end)
	}()

	warmupShortCirc := true
	nextOp := true
	next = time.Now()

	for time.Now().UnixNano() < end.UnixNano() {
		if warmupShortCirc && time.Now().UnixNano() > warmpupEnd.UnixNano() {
			fmt.Println("//////// warmupDone")
			warmupShortCirc = false
			st = time.Now()
			opCnt = 0
		}

		now = time.Now()
		if next.UnixNano() > now.UnixNano() {
			if now.UnixNano() > end.UnixNano() {
				break
			}
			continue
		}

		if limitThreads {
			if nextOp {
				nextOp = false
				op = g.workload.NextOp()
			}
		} else {
			op = g.workload.NextOp()
		}

		if limitThreads {
			switch op.(type) {
			case operations.Frontpage, operations.Story:
				//			val := atomic.AddInt64(&inFlightR, 1)
				//			if val > maxInFlightR {
				//				maxInFlightR = val
				//			}
				select {
				case limitReadCh <- struct{}{}:
					nextOp = true
					opID++
				default:
					continue
				}
			case operations.StoryVote, operations.CommentVote, operations.Submit, operations.Comment:
				select {
				case limitWriteCh <- struct{}{}:
					nextOp = true
				default:
					continue
				}
			}
		}

		go doOperationAsync(op, measurementsCh, limitReadCh, limitWriteCh, limitThreads, nil, opID)

		opCnt++

		next = next.Add(interArrival)
	}
	en := time.Now()
	runtime := en.Sub(st)

	wg.Wait()

	//	fmt.Println("max in flight: ", maxInFlightR)

	return measurements.ClientMeasurements{
		Runtime:        runtime,
		OpsOffered:     opCnt,
		DeadlockAborts: deadlockAborts,
		Histograms:     histograms,
	}
}

func doOperationAsync(op operations.Operation, measurementsCh chan measurements.Measurement, limitReadCh, limitWriteCh chan struct{}, limitThreads bool, inFlightR *int64, opID int64) {
	opType, respTime, endTs := op.DoOperation(opID)

	if limitThreads {
		switch op.(type) {
		case operations.Frontpage, operations.Story:
			<-limitReadCh
		//	atomic.AddInt64(inFlightR, -1)
		case operations.StoryVote, operations.CommentVote, operations.Submit, operations.Comment:
			<-limitWriteCh
		}
	}

	measurementsCh <- measurements.Measurement{
		RespTime: respTime,
		OpType:   opType,
		EndTs:    endTs,
	}
}

func measurementsConsumer(measurementsCh chan measurements.Measurement, histograms map[string]*stats.Histogram, deadlockAborts *int64, warmupEnd, end time.Time) {
	for i, t := 0, time.NewTimer(2*time.Second); true; i++ {
		select {
		case m, isopen := <-measurementsCh:
			if !isopen {
				return
			}
			if m.OpType == measurements.Deadlock {
				*deadlockAborts++
			} else {
				if m.EndTs.UnixNano() > warmupEnd.UnixNano() && m.EndTs.UnixNano() < end.UnixNano() {
					if m.OpType == measurements.Write {
						histograms["write"].Add(m.RespTime.Nanoseconds())
					} else {
						histograms["read"].Add(m.RespTime.Nanoseconds())
					}
				}
			}
			t.Reset(2 * time.Second)
		case <-t.C:
			return
		}
	}
}

// Preload ...
func (g *Generator) Preload() error {
	return g.workload.Preload()
}

// Close ...
func (g *Generator) Close() {
	g.workload.Close()
}

// Test ...
func (g *Generator) Test() error {
	return g.workload.Test()
}
