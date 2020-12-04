package measurements

import (
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc/benchmark/stats"
)

// Measurements ...
type Measurements struct {
	sync.Mutex
	clientMeasurements []ClientMeasurements
}

// ClientMeasurements ...
type ClientMeasurements struct {
	Runtime        time.Duration
	OpsOffered     int64
	DeadlockAborts int64
	Histograms     map[string]*stats.Histogram
}

// OpType ..
type OpType int

const (
	// Read ...
	Read OpType = iota
	// Write ...
	Write OpType = iota
	// Done ...
	Done OpType = iota
	// Deadlock ...
	Deadlock OpType = iota
)

// Measurement ...
type Measurement struct {
	RespTime time.Duration
	OpType   OpType
	EndTs    time.Time
}

// Metrics ...
type Metrics struct {
	Runtime        time.Duration
	LoadOffered    float64
	Throughput     float64
	PerOpMetrics   map[string]OpMetrics
	DeadlockAborts int64
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

var (
	histogramOpts = stats.HistogramOptions{
		// up to 500ms
		NumBuckets:   50000,
		GrowthFactor: .01,
	}
)

// New ...
func New() *Measurements {
	return &Measurements{
		clientMeasurements: make([]ClientMeasurements, 0),
	}
}

// NewHistogram ...
func NewHistogram() *stats.Histogram {
	return stats.NewHistogram(histogramOpts)
}

//ReportMeasurements ...
func (p *Measurements) ReportMeasurements(m ClientMeasurements) {
	p.Lock()
	p.clientMeasurements = append(p.clientMeasurements, m)
	p.Unlock()
}

// CalculateMetrics ...
func (p *Measurements) CalculateMetrics(fTRead, fTWrite *os.File) (Metrics, error) {
	var aggOpsOffered int64
	var aggDeadlockAborts int64
	var aggRuntime time.Duration

	aggHistograms := make(map[string]*stats.Histogram)
	aggHistograms["read"] = stats.NewHistogram(histogramOpts)
	aggHistograms["write"] = stats.NewHistogram(histogramOpts)

	traceF := make(map[string]*os.File)
	traceF["read"] = fTRead
	traceF["write"] = fTWrite

	for _, c := range p.clientMeasurements {
		aggRuntime += c.Runtime
		aggOpsOffered += c.OpsOffered
		aggDeadlockAborts += c.DeadlockAborts

		for opType, hist := range c.Histograms {
			aggHistograms[opType].Merge(hist)
		}

	}

	m := Metrics{
		PerOpMetrics: make(map[string]OpMetrics),
	}

	m.Runtime = aggRuntime / time.Duration(len(p.clientMeasurements))
	m.LoadOffered = float64(aggOpsOffered) / aggRuntime.Seconds() * float64(len(p.clientMeasurements))

	var totalOpCnt int64
	for opType, hist := range aggHistograms {
		totalOpCnt += hist.Count
		opMetrics := OpMetrics{
			OpCount:    hist.Count,
			Throughput: float64(hist.Count) / aggRuntime.Seconds(),
			P50:        durationToMillis(time.Duration(pepcentile(.5, hist))),
			P90:        durationToMillis(time.Duration(pepcentile(.9, hist))),
			P95:        durationToMillis(time.Duration(pepcentile(.95, hist))),
			P99:        durationToMillis(time.Duration(pepcentile(.99, hist))),
		}

		if _, err := fmt.Fprintf(traceF[opType], "%.5f\n", aggRuntime.Seconds()); err != nil {
			return m, err
		}
		if _, err := fmt.Fprintf(traceF[opType], "%d\n", hist.Count); err != nil {
			return m, err
		}

		m.PerOpMetrics[opType] = opMetrics
	}

	m.Throughput = float64(totalOpCnt) / aggRuntime.Seconds() * float64(len(p.clientMeasurements))

	m.DeadlockAborts = aggDeadlockAborts

	return m, nil
}

func pepcentile(percentile float64, h *stats.Histogram) int64 {
	percentileCount := int64(float64(h.Count) * percentile)
	currentCount := int64(0)
	for _, bucket := range h.Buckets {
		if currentCount+bucket.Count >= percentileCount {
			lastBuckedFilled := float64(percentileCount-currentCount) / float64(bucket.Count)
			return int64((1.0-lastBuckedFilled)*bucket.LowBound + lastBuckedFilled*bucket.LowBound*(1.0+histogramOpts.GrowthFactor))
		}
		currentCount += bucket.Count
	}
	panic("should have found a bound")
}

func durationToMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
