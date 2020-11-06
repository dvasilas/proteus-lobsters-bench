package perf

import (
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/benchmark/stats"
)

type durations []time.Duration

func (d durations) Len() int           { return len(d) }
func (d durations) Less(i, j int) bool { return int64(d[i]) < int64(d[j]) }
func (d durations) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }

func (d durations) percentile(p float64) time.Duration {
	return d[int(float64(d.Len())*p+0.5)-1]
}

type threadRawMeasurements struct {
	buf            map[string][]time.Duration
	runtime        time.Duration
	opsOffered     int64
	deadlockAborts int64
	hist           map[string]*stats.Histogram
}

var (
	hopts = stats.HistogramOptions{
		// up to 500ms
		NumBuckets:   50000,
		GrowthFactor: .01,
	}
)

// Perf ...
type Perf struct {
	sync.Mutex
	measurementsBuf []threadRawMeasurements
}

// OpMetrics ...
type OpMetrics struct {
	OpCount        int64
	OpCounthist    int64
	Throughput     float64
	Throughputhist float64
	P50            float64
	P90            float64
	P95            float64
	P99            float64
	P50hist        float64
	P90hist        float64
	P95hist        float64
	P99hist        float64
}

// Metrics ...
type Metrics struct {
	Runtime        time.Duration
	LoadOffered    float64
	Throughput     float64
	PerOpMetrics   map[string]OpMetrics
	DeadlockAborts int64
}

// New ...
func New() *Perf {
	return &Perf{
		measurementsBuf: make([]threadRawMeasurements, 0),
	}
}

//ReportMeasurements ...
func (p *Perf) ReportMeasurements(runtime time.Duration, opsOffered int64, m map[string][]time.Duration, deadlockAborts int64, hist map[string]*stats.Histogram) {
	p.Lock()
	p.measurementsBuf = append(p.measurementsBuf, threadRawMeasurements{
		buf:            m,
		runtime:        runtime,
		opsOffered:     opsOffered,
		deadlockAborts: deadlockAborts,
		hist:           hist,
	})
	p.Unlock()
}

// CalculateMetrics ...
func (p *Perf) CalculateMetrics() Metrics {
	var totalOpsOffered int64
	var totalDeadlockAborts int64
	var totalRuntime time.Duration
	aggregateMeasurements := make(map[string]durations)

	var h map[string]*stats.Histogram

	for _, threadReport := range p.measurementsBuf {
		totalRuntime += threadReport.runtime
		totalOpsOffered += threadReport.opsOffered

		for measurementType, rawMeasurements := range threadReport.buf {
			aggregateMeasurements[measurementType] = append(
				aggregateMeasurements[measurementType],
				rawMeasurements...,
			)
		}

		totalDeadlockAborts += threadReport.deadlockAborts

		h = threadReport.hist

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
			hist := h[opType]
			opMetrics := OpMetrics{
				OpCount:        int64(len(threadMeasurements)),
				OpCounthist:    hist.Count,
				Throughput:     float64(len(threadMeasurements)) / totalRuntime.Seconds() * float64(len(p.measurementsBuf)),
				Throughputhist: float64(hist.Count) / totalRuntime.Seconds(),
				P50:            durationToMillis(threadMeasurements[threadMeasurements.Len()/2]),
				P50hist:        durationToMillis(time.Duration(pepcentile(.5, hist))),
				P90:            durationToMillis(threadMeasurements.percentile(0.9)),
				P90hist:        durationToMillis(time.Duration(pepcentile(.9, hist))),
				P95:            durationToMillis(threadMeasurements.percentile(0.95)),
				P95hist:        durationToMillis(time.Duration(pepcentile(.95, hist))),
				P99:            durationToMillis(threadMeasurements.percentile(0.99)),
				P99hist:        durationToMillis(time.Duration(pepcentile(.99, hist))),
			}

			m.PerOpMetrics[opType] = opMetrics

		}
	}

	m.Throughput = float64(totalOpCnt) / totalRuntime.Seconds() * float64(len(p.measurementsBuf))

	m.DeadlockAborts = totalDeadlockAborts

	return m
}

func pepcentile(percentile float64, h *stats.Histogram) int64 {
	percentileCount := int64(float64(h.Count) * percentile)
	currentCount := int64(0)
	for _, bucket := range h.Buckets {
		if currentCount+bucket.Count >= percentileCount {
			lastBuckedFilled := float64(percentileCount-currentCount) / float64(bucket.Count)
			return int64((1.0-lastBuckedFilled)*bucket.LowBound + lastBuckedFilled*bucket.LowBound*(1.0+hopts.GrowthFactor))
		}
		currentCount += bucket.Count
	}
	panic("should have found a bound")
}

func durationToMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
