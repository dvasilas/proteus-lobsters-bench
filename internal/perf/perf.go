package perf

import (
	"sort"
	"sync"
	"time"
)

type durations []time.Duration

func (d durations) Len() int           { return len(d) }
func (d durations) Less(i, j int) bool { return int64(d[i]) < int64(d[j]) }
func (d durations) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }

func (d durations) percentile(p float64) time.Duration {
	return d[int(float64(d.Len())*p+0.5)-1]
}

type threadRawMeasurements struct {
	buf        map[string][]time.Duration
	runtime    time.Duration
	opsOffered int64
}

// Perf ...
type Perf struct {
	sync.Mutex
	measurementsBuf []threadRawMeasurements
}

// OpMetrics ...
type OpMetrics struct {
	OpCount    int64
	Throughput float64
	P50        float64
	P90        float64
	P95        float64
	P99        float64
}

// Metrics ...
type Metrics struct {
	Runtime      time.Duration
	LoadOffered  float64
	Throughput   float64
	PerOpMetrics map[string]OpMetrics
}

// New ...
func New() *Perf {
	return &Perf{
		measurementsBuf: make([]threadRawMeasurements, 0),
	}
}

//ReportMeasurements ...
func (p *Perf) ReportMeasurements(runtime time.Duration, opsOffered int64, m map[string][]time.Duration) {
	p.Lock()
	p.measurementsBuf = append(p.measurementsBuf, threadRawMeasurements{
		buf:        m,
		runtime:    runtime,
		opsOffered: opsOffered,
	})
	p.Unlock()
}

// CalculateMetrics ...
func (p *Perf) CalculateMetrics() Metrics {
	var totalOpsOffered int64
	var totalRuntime time.Duration
	aggregateMeasurements := make(map[string]durations)

	for _, threadReport := range p.measurementsBuf {
		totalRuntime += threadReport.runtime
		totalOpsOffered += threadReport.opsOffered

		for measurementType, rawMeasurements := range threadReport.buf {
			aggregateMeasurements[measurementType] = append(
				aggregateMeasurements[measurementType],
				rawMeasurements...,
			)
		}
	}

	for _, threadMeasurements := range aggregateMeasurements {
		sort.Sort(threadMeasurements)
	}

	m := Metrics{
		PerOpMetrics: make(map[string]OpMetrics),
	}

	m.Runtime = totalRuntime / time.Duration(len(p.measurementsBuf))
	m.LoadOffered = float64(totalOpsOffered) / totalRuntime.Seconds() * float64(len(p.measurementsBuf))

	totalOpCnt := 0
	for opType, threadMeasurements := range aggregateMeasurements {
		totalOpCnt += len(threadMeasurements)
		if len(threadMeasurements) > 0 {
			opMetrics := OpMetrics{
				OpCount:    int64(len(threadMeasurements)),
				Throughput: float64(len(threadMeasurements)) / totalRuntime.Seconds() * float64(len(p.measurementsBuf)),
				P50:        durationToMillis(threadMeasurements[threadMeasurements.Len()/2]),
				P90:        durationToMillis(threadMeasurements.percentile(0.9)),
				P95:        durationToMillis(threadMeasurements.percentile(0.95)),
				P99:        durationToMillis(threadMeasurements.percentile(0.99)),
			}
			m.PerOpMetrics[opType] = opMetrics
		}
	}

	m.Throughput = float64(totalOpCnt) / totalRuntime.Seconds() * float64(len(p.measurementsBuf))

	return m
}

func durationToMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
