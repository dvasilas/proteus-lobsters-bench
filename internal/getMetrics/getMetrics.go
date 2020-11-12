package getmetrics

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	proteusclient "github.com/dvasilas/proteus/pkg/proteus-go-client"
)

// GetMetrics ...
func GetMetrics(conf config.BenchmarkConfig) error {
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
		fmt.Printf("[notificationLatency-%s] p50(ms): %.5f\n", q.Name, resp.NotificationLatencyP50)
		fmt.Printf("[notificationLatency-%s] p90(ms): %.5f\n", q.Name, resp.NotificationLatencyP90)
		fmt.Printf("[notificationLatency-%s] p95(ms): %.5f\n", q.Name, resp.NotificationLatencyP95)
		fmt.Printf("[notificationLatency-%s] p99(ms): %.5f\n", q.Name, resp.NotificationLatencyP99)

	}

	return nil

}
