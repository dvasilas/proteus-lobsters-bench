package getmetrics

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	proteusclient "github.com/dvasilas/proteus/pkg/proteus-go-client"
)

// GetMetrics ...
func GetMetrics(conf config.BenchmarkConfig, fM *os.File) error {
	for _, q := range conf.GetMetrics.QPU {
		endpoint := strings.Split(q.Endpoint, ":")
		port, err := strconv.ParseInt(endpoint[1], 10, 64)
		if err != nil {
			return err
		}

		c, err := proteusclient.NewClient(proteusclient.Host{Name: endpoint[0], Port: int(port)}, 1, 1, false)
		if err != nil {
			return err
		}

		resp, err := c.GetMetrics()
		if err != nil {
			return err

		}
		if _, err := fmt.Fprintf(fM, "[notificationLatency-%s] p50(ms): %.5f\n", q.Name, resp.NotificationLatencyP50); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[notificationLatency-%s] p90(ms): %.5f\n", q.Name, resp.NotificationLatencyP90); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[notificationLatency-%s] p95(ms): %.5f\n", q.Name, resp.NotificationLatencyP95); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[notificationLatency-%s] p99(ms): %.5f\n", q.Name, resp.NotificationLatencyP99); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[processingLatency-%s] p50(ms): %.5f\n", q.Name, resp.ProcessingLatencyP50); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[processingLatency-%s] p90(ms): %.5f\n", q.Name, resp.ProcessingLatencyP90); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[processingLatency-%s] p95(ms): %.5f\n", q.Name, resp.ProcessingLatencyP95); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[processingLatency-%s] p99(ms): %.5f\n", q.Name, resp.ProcessingLatencyP99); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[stateUpdateLatency-%s] p50(ms): %.5f\n", q.Name, resp.StateUpdateLatencyP50); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[stateUpdateLatency-%s] p90(ms): %.5f\n", q.Name, resp.StateUpdateLatencyP90); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[stateUpdateLatency-%s] p95(ms): %.5f\n", q.Name, resp.StateUpdateLatencyP95); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[stateUpdateLatency-%s] p99(ms): %.5f\n", q.Name, resp.StateUpdateLatencyP99); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[freshnessLatency-%s] p50(ms): %.5f\n", q.Name, resp.FreshnessLatencyP50); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[freshnessLatency-%s] p90(ms): %.5f\n", q.Name, resp.FreshnessLatencyP90); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[freshnessLatency-%s] p95(ms): %.5f\n", q.Name, resp.FreshnessLatencyP95); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[freshnessLatency-%s] p99(ms): %.5f\n", q.Name, resp.FreshnessLatencyP99); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[FreshnessVersions-%s] 0: %.5f\n", q.Name, resp.FreshnessVersions0); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[FreshnessVersions-%s] 1: %.5f\n", q.Name, resp.FreshnessVersions1); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[FreshnessVersions-%s] 2: %.5f\n", q.Name, resp.FreshnessVersions2); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[FreshnessVersions-%s] 4: %.5f\n", q.Name, resp.FreshnessVersions4); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[DataTransfer-%s] (kB): %.5f\n", q.Name, resp.KBytesSent); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[responseTime-%s] p50(ms): %.5f\n", q.Name, resp.ResponseTimeP50); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[responseTime-%s] p90(ms): %.5f\n", q.Name, resp.ResponseTimeP90); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[responseTime-%s] p95(ms): %.5f\n", q.Name, resp.ResponseTimeP95); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(fM, "[responseTime-%s] p99(ms): %.5f\n", q.Name, resp.ResponseTimeP99); err != nil {
			return err
		}
	}

	return nil

}
