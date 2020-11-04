package config

import (
	"fmt"
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

// BenchmarkConfig ...
type BenchmarkConfig struct {
	Tracing bool
	Preload struct {
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
		WriteRatio    float64
		DownVoteRatio float64
	}
	Benchmark struct {
		DoPreload        bool
		DoWarmup         bool
		Runtime          int
		Warmup           int
		ThreadCount      int
		OpCount          int64
		MeasuredSystem   string
		TargetLoad       int64
		MaxInFlightRead  int64
		MaxInFlightWrite int64
	}
	Connection struct {
		ProteusEndpoint string
		DBEndpoint      string
		Database        string
		AccessKeyID     string
		SecretAccessKey string
		PoolSize        int
		PoolOverflow    int
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
func (c *BenchmarkConfig) Print() {
	fmt.Printf("Target system: %s\n", c.Benchmark.MeasuredSystem)
	fmt.Printf("Benchmark duration(s): %d\n", c.Benchmark.Runtime)
	fmt.Printf("Warmup(s): %d\n", c.Benchmark.Warmup)
	fmt.Printf("Benchmark threads: %d\n", c.Benchmark.ThreadCount)
	fmt.Printf("Target load: %d\n", c.Benchmark.TargetLoad*int64(c.Benchmark.ThreadCount))
	fmt.Printf("Max in flight read: %d\n", c.Benchmark.MaxInFlightRead)
	fmt.Printf("Max in flight write: %d\n", c.Benchmark.MaxInFlightWrite)
	fmt.Printf("Conn pool size: %d\n", c.Connection.PoolSize+c.Connection.PoolOverflow)
	fmt.Printf("[workload] Q/W ratio(%%): %f\n", 1-c.Operations.WriteRatio)
	fmt.Printf("[workload] U/D vote ratio(%%): %f\n", 1-c.Operations.DownVoteRatio)
	fmt.Printf("[preload] Users: %d\n", c.Preload.RecordCount.Users)
	fmt.Printf("[preload] Stories: %d\n", c.Preload.RecordCount.Stories)
	fmt.Printf("[preload] Comments: %d\n", c.Preload.RecordCount.Comments)
}
