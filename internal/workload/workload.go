package workload

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"

	"time"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	"github.com/dvasilas/proteus-lobsters-bench/internal/operations"
)

// Workload ...
type Workload struct {
	config   *config.BenchmarkConfig
	ops      *operations.Operations
	workload workload
}

type workload interface {
	nextOp() operations.Operation
}

// NewWorkload ...
func NewWorkload(conf *config.BenchmarkConfig) (*Workload, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	ops, err := operations.NewOperations(conf)
	if err != nil {
		return nil, err
	}

	var w workload
	switch conf.Benchmark.WorkloadType {
	case "simple":
		w = newWorkloadSimple(conf, ops)
	case "complete":
		w = newWorkloadComplete(ops)
	default:
		return nil, errors.New("unknown workload type")
	}

	return &Workload{
		ops:      ops,
		workload: w,
		config:   conf,
	}, nil
}

// NextOp ...
func (w *Workload) NextOp() operations.Operation {
	return w.workload.nextOp()
}

type workloadSimple struct {
	writeRatio    float64
	downVoteRatio float64
	ops           *operations.Operations
}

func newWorkloadSimple(conf *config.BenchmarkConfig, ops *operations.Operations) workloadSimple {
	return workloadSimple{
		writeRatio:    conf.Operations.WriteRatio,
		downVoteRatio: conf.Operations.DownVoteRatio,
		ops:           ops,
	}
}

func (w workloadSimple) nextOp() operations.Operation {
	r := rand.Float64()

	if r < w.writeRatio {
		vote := rand.Float64()
		if vote < w.downVoteRatio {
			return operations.StoryVote{Ops: w.ops, Vote: -1}
		}
		return operations.StoryVote{Ops: w.ops, Vote: 1}
	}

	return operations.Frontpage{Ops: w.ops}
}

type workloadComplete struct {
	ops *operations.Operations
}

func newWorkloadComplete(ops *operations.Operations) workloadComplete {
	return workloadComplete{
		ops: ops,
	}
}

func (w workloadComplete) nextOp() operations.Operation {
	for true {
		seed := rand.Intn(100000)
		// 	55.842%  GET   /stories/X
		//  30.105%  GET   /
		//   6.702%  GET   /u/X
		//   4.674%  GET   /comments[/X]
		//   0.967%  GET   /recent[/X]
		//   0.630%  POST  /comments/X/upvote
		//   0.475%  POST  /stories/X/upvote
		//   0.316%  POST  /comments
		//   0.087%  POST  /login
		//   0.071%  POST  /comments/X
		//   0.054%  POST  /comments/X/downvote
		//   0.053%  POST  /stories
		//   0.021%  POST  /stories/X/downvote
		//   0.003%  POST  /logout
		if applies(55842, &seed) {
			// /stories/X
			return operations.Story{Ops: w.ops}
		} else if applies(30105, &seed) {
			// /
			return operations.Frontpage{Ops: w.ops}
		} else if applies(6702, &seed) {
			// /u/X
			continue
		} else if applies(4674, &seed) {
			// /comments[/X]
			continue
		} else if applies(967, &seed) {
			// /recent[/X]
			continue
		} else if applies(630, &seed) {
			// /comments/X/upvote
			return operations.CommentVote{Ops: w.ops, Vote: 1}
		} else if applies(475, &seed) {
			// /stories/X/upvote
			return operations.StoryVote{Ops: w.ops, Vote: 1}
		} else if applies(316, &seed) {
			// /comments
			return operations.Comment{Ops: w.ops}
		} else if applies(87, &seed) {
			// /login
			continue
		} else if applies(71, &seed) {
			// /comments/X
			continue
		} else if applies(54, &seed) {
			// /comments/X/downvote
			return operations.CommentVote{Ops: w.ops, Vote: -1}
		} else if applies(53, &seed) {
			// /stories
			return operations.Submit{Ops: w.ops}
		} else if applies(21, &seed) {
			// /stories/X/downvote
			return operations.StoryVote{Ops: w.ops, Vote: -1}
		} else {
			// /logout
			continue
		}
	}
	return operations.Frontpage{}
}

// Preload ...
func (w Workload) Preload() error {
	fmt.Println("Preloading ..")

	w.ops.StoryID = 0

	preadloadThreads := 10
	var wg sync.WaitGroup

	for t := 1; t <= preadloadThreads; t++ {
		wg.Add(1)
		go func(count int64) {
			defer wg.Done()
			for i := int64(0); i < count; i++ {
				if err := w.ops.AddUser(); err != nil {
					panic(err)
				}
			}
		}(w.config.Preload.RecordCount.Users / int64(preadloadThreads))
	}

	wg.Wait()
	fmt.Printf("Created %d users\n", w.config.Preload.RecordCount.Users)

	for t := 1; t <= preadloadThreads; t++ {
		wg.Add(1)
		go func(count int64) {
			defer wg.Done()
			for i := int64(0); i < count; i++ {
				if _, err := w.ops.Submit(); err != nil {
					panic(err)
				}
			}

		}(w.config.Preload.RecordCount.Stories / int64(preadloadThreads))
	}

	wg.Wait()
	fmt.Printf("Created %d stories\n", w.config.Preload.RecordCount.Stories)

	for t := 1; t <= preadloadThreads; t++ {
		wg.Add(1)
		go func(count int64) {
			defer wg.Done()
			for i := int64(0); i < count; i++ {
				if _, err := w.ops.Comment(); err != nil {
					panic(err)
				}
			}
		}(w.config.Preload.RecordCount.Comments / int64(preadloadThreads))
	}

	wg.Wait()
	fmt.Printf("Created %d comments\n", w.config.Preload.RecordCount.Comments)

	for t := 1; t <= preadloadThreads; t++ {
		wg.Add(1)
		go func(count int64) {
			defer wg.Done()
			for i := int64(0); i < count; i++ {
				if _, err := w.ops.StoryVote(1); err != nil {
					panic(err)
				}
			}
		}(w.config.Preload.RecordCount.Votes / int64(preadloadThreads))
	}

	wg.Wait()
	fmt.Printf("Created %d votes\n", w.config.Preload.RecordCount.Votes)

	fmt.Println("Preloading done")
	return nil
}

// Test ...
func (w Workload) Test() error {
	fmt.Println("Submit Story ...")
	if _, err := w.ops.Submit(); err != nil {
		return err
	}

	fmt.Println("GetHomepage ...")
	_, err := w.ops.Frontpage()
	if err != nil {
		return err
	}

	fmt.Println("UpVote story ...")
	if _, err := w.ops.StoryVote(1); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)

	fmt.Println("Get Homepage ...")
	_, err = w.ops.Frontpage()
	if err != nil {
		return err
	}

	fmt.Println("Get story by storyID ...")
	_, err = w.ops.Story()
	if err != nil {
		return err
	}

	return nil
}

// Close ...
func (w Workload) Close() {
	w.ops.Close()
}

func applies(bound int, n *int) bool {
	f := *n <= bound
	*n -= bound
	return f
}
