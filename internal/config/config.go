package config

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
)

// DistributionType ...
type DistributionType int

const (
	// Uniform ...
	Uniform DistributionType = iota
	// Histogram ...
	Histogram DistributionType = iota
	// VoteTopStories ...
	VoteTopStories DistributionType = iota
)

// BenchmarkConfig ...
type BenchmarkConfig struct {
	Tracing         bool
	WorkerPoolSizeQ int
	JobQueueSizeQ   int
	WorkerPoolSizeW int
	JobQueueSizeW   int
	Preload         struct {
		RecordCount struct {
			Users    int64
			Stories  int64
			Comments int64
			Votes    int64
		}
	}
	Operations struct {
		Homepage struct {
			StoriesLimit int
		}
		WriteRatio       float64
		DownVoteRatio    float64
		DistributionType string
	}
	Benchmark struct {
		DoPreload        bool
		DoWarmup         bool
		Runtime          int
		Warmup           int
		ThreadCount      int
		MeasuredSystem   string
		TargetLoad       int64
		WorkloadType     string
		MaxInFlightRead  int64
		MaxInFlightWrite int64
	}
	Connection struct {
		ProteusEndpoints  []string
		LobstersEndpoints []string
		DBEndpoint        string
		Database          string
		AccessKeyID       string
		SecretAccessKey   string
		PoolSize          int
		PoolOverflow      int
	}
	GetMetrics struct {
		QPU []struct {
			Name     string
			Endpoint string
		}
	}
	Distributions struct {
		VotesPerStory []struct {
			Bin   int64
			Count int64
		}
		VotesPerComment []struct {
			Bin   int64
			Count int64
		}
		CommentsPerStory []struct {
			Bin   int64
			Count int64
		}
	}
}

// GetConfig ...
func GetConfig(configFile string) (BenchmarkConfig, error) {
	config := BenchmarkConfig{}
	err := readConfigFile(configFile, &config)

	return config, err
}

func readConfigFile(configFile string, conf *BenchmarkConfig) error {
	configData, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil
	}
	return toml.Unmarshal(configData, conf)
}

// Print ...
func (c *BenchmarkConfig) Print(f *os.File) error {
	if _, err := fmt.Fprintf(f, "Target system: %s\n", c.Benchmark.MeasuredSystem); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "Benchmark duration(s): %d\n", c.Benchmark.Runtime); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "Warmup(s): %d\n", c.Benchmark.Warmup); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "Benchmark threads: %d\n", c.Benchmark.ThreadCount); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "Target load: %d\n", c.Benchmark.TargetLoad*int64(c.Benchmark.ThreadCount)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "Max in flight read: %d\n", c.Benchmark.MaxInFlightRead); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "Max in flight write: %d\n", c.Benchmark.MaxInFlightWrite); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "Conn pool size: %d\n", c.Connection.PoolSize+c.Connection.PoolOverflow); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "[workload] Q/W ratio(%%): %f\n", 1-c.Operations.WriteRatio); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "[workload] U/D vote ratio(%%): %f\n", 1-c.Operations.DownVoteRatio); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "[preload] Users: %d\n", c.Preload.RecordCount.Users); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "[preload] Stories: %d\n", c.Preload.RecordCount.Stories); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "[preload] Comments: %d\n", c.Preload.RecordCount.Comments); err != nil {
		return err
	}

	return nil
}
