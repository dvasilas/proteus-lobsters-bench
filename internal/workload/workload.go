package workload

import (
	"fmt"
	"log"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	"github.com/dvasilas/proteus-lobsters-bench/internal/distributions"
	"github.com/dvasilas/proteus-lobsters-bench/internal/operations"
	"github.com/dvasilas/proteus-lobsters-bench/internal/perf"
)

// Workload ...
type Workload struct {
	config              *config.BenchmarkConfig
	ops                 *operations.Operations
	measurements        *perf.Perf
	storyVoteSampler    distributions.Sampler
	commentVoteSampler  distributions.Sampler
	commentStorySampler distributions.Sampler
	storyID             int64
}

// NewWorkload ...
func NewWorkload(conf *config.BenchmarkConfig) (*Workload, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	ops, err := operations.NewOperations(conf)
	if err != nil {
		return nil, err
	}

	return &Workload{
		config:              conf,
		ops:                 ops,
		measurements:        perf.New(),
		storyVoteSampler:    distributions.NewSampler(votesPerStory),
		commentVoteSampler:  distributions.NewSampler(votesPerComment),
		commentStorySampler: distributions.NewSampler(commentsPerStory),
		storyID:             conf.Preload.RecordCount.Stories,
	}, nil
}

// RunMicro ...
func (w Workload) RunMicro(measurementBufferSize int64) (map[string][]time.Duration, map[string]int64, time.Time, time.Time) {
	durations := make(map[string][]time.Duration, measurementBufferSize)
	durations["getHomepage"] = make([]time.Duration, measurementBufferSize)
	durations["vote"] = make([]time.Duration, measurementBufferSize)

	perOpCnt := make(map[string]int64)
	perOpCnt["getHomepage"] = 0
	perOpCnt["vote"] = 0
	var opCnt int64
	var st time.Time
	var respTime time.Duration
	var err error
	timerStarted := false
	warmingUp, warmupTimeout := w.config.Benchmark.DoWarmup, time.After(time.Duration(w.config.Benchmark.Warmup)*time.Second)
	for timeIsUp, timeout := true, time.After(time.Duration(w.config.Benchmark.Runtime)*time.Second); timeIsUp; {

		select {
		case <-timeout:
			timeIsUp = false
		case <-warmupTimeout:
			warmingUp = false
		default:
		}

		if !timerStarted && !warmingUp {
			timerStarted = true
			st = time.Now()
		}
		if opCnt == w.config.Benchmark.OpCount {
			break
		}

		r := rand.Float64()
		if r < w.config.Operations.WriteRatio {
			vote := rand.Float64()
			if vote < w.config.Operations.DownVoteRatio {
				respTime, err = w.StoryVote(-1)
				if err != nil {
					er(err)
				}
			} else {
				respTime, err = w.StoryVote(1)
				if err != nil {
					er(err)
				}
			}
			if !warmingUp {
				durations["vote"][perOpCnt["vote"]] = respTime
				perOpCnt["vote"]++
			}
		} else {
			respTime, err = w.Frontpage()
			if err != nil {
				er(err)
			}
			if !warmingUp {
				durations["getHomepage"][perOpCnt["getHomepage"]] = respTime
				perOpCnt["getHomepage"]++
			}
		}
		opCnt++
	}
	return durations, perOpCnt, st, time.Now()
}

func applies(bound int, n *int) bool {
	f := *n <= bound
	*n -= bound
	return f
}

// RunMacro ...
func (w Workload) RunMacro(measurementBufferSize int64) (map[string][]time.Duration, map[string]int64, time.Time, time.Time) {
	durations := make(map[string][]time.Duration, measurementBufferSize)
	durations["read"] = make([]time.Duration, measurementBufferSize)
	durations["write"] = make([]time.Duration, measurementBufferSize)

	perOpCnt := make(map[string]int64)
	perOpCnt["read"] = 0
	perOpCnt["write"] = 0
	var opCnt int64
	var st time.Time
	var respTime time.Duration
	var err error
	timerStarted := false
	warmingUp, warmupTimeout := w.config.Benchmark.DoWarmup, time.After(time.Duration(w.config.Benchmark.Warmup)*time.Second)
	for timeIsUp, timeout := true, time.After(time.Duration(w.config.Benchmark.Runtime)*time.Second); timeIsUp; {
		select {
		case <-timeout:
			timeIsUp = false
		case <-warmupTimeout:
			warmingUp = false
		default:
		}

		if !timerStarted && !warmingUp {
			timerStarted = true
			st = time.Now()
		}
		if opCnt == w.config.Benchmark.OpCount {
			break
		}

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
			respTime, err = w.Story()
			if err != nil {
				er(err)
			}
			if !warmingUp {
				durations["read"][perOpCnt["read"]] = respTime
				perOpCnt["read"]++
			}
		} else if applies(30105, &seed) {
			// /
			respTime, err = w.Frontpage()
			if err != nil {
				er(err)
			}
			if !warmingUp {
				durations["read"][perOpCnt["read"]] = respTime
				perOpCnt["read"]++
			}
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
			respTime, err = w.CommentVote(1)
			if err != nil {
				er(err)
			}
			if !warmingUp {
				durations["write"][perOpCnt["write"]] = respTime
				perOpCnt["write"]++
			}
		} else if applies(475, &seed) {
			// /stories/X/upvote
			respTime, err = w.StoryVote(1)
			if err != nil {
				er(err)
			}
			if !warmingUp {
				durations["write"][perOpCnt["write"]] = respTime
				perOpCnt["write"]++
			}
		} else if applies(316, &seed) {
			// /comments
			respTime, err = w.Comment()
			if err != nil {
				er(err)
			}
			if !warmingUp {
				durations["write"][perOpCnt["write"]] = respTime
				perOpCnt["write"]++
			}
		} else if applies(87, &seed) {
			// /login
			continue
		} else if applies(71, &seed) {
			// /comments/X
			continue
		} else if applies(54, &seed) {
			// /comments/X/downvote
			respTime, err = w.CommentVote(-1)
			if err != nil {
				er(err)
			}
			if !warmingUp {
				durations["write"][perOpCnt["write"]] = respTime
				perOpCnt["write"]++
			}
		} else if applies(53, &seed) {
			// /stories
			id := atomic.AddInt64(&w.storyID, 1)
			respTime, err = w.Submit(id)
			if err != nil {
				er(err)
			}
			if !warmingUp {
				durations["write"][perOpCnt["write"]] = respTime
				perOpCnt["write"]++
			}
		} else if applies(21, &seed) {
			// /stories/X/downvote
			respTime, err = w.StoryVote(-1)
			if err != nil {
				er(err)
			}
			if !warmingUp {
				durations["write"][perOpCnt["write"]] = respTime
				perOpCnt["write"]++
			}
		} else {
			// /logout
			continue
		}
		opCnt++
	}
	return durations, perOpCnt, st, time.Now()
}

// Test ...
func (w Workload) Test() error {
	return w.ops.Test()
}

// Preload ...
func (w Workload) Preload() error {
	fmt.Println("Preloading ..")

	w.storyID = 0

	preadloadThreads := 10
	var wg sync.WaitGroup

	for t := 1; t <= preadloadThreads; t++ {
		wg.Add(1)
		go func(count int64) {
			defer wg.Done()
			for i := int64(0); i < count; i++ {
				if err := w.AddUser(); err != nil {
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
				id := atomic.AddInt64(&w.storyID, 1)
				if _, err := w.Submit(id); err != nil {
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
				if _, err := w.Comment(); err != nil {
					panic(err)
				}
			}
		}(w.config.Preload.RecordCount.Comments / int64(preadloadThreads))
	}

	wg.Wait()
	fmt.Printf("Created %d comments\n", w.config.Preload.RecordCount.Comments)

	fmt.Println("Preloading done")
	return nil
}

// Frontpage ...
func (w *Workload) Frontpage() (time.Duration, error) {
	st := time.Now()

	_, err := w.ops.Frontpage()

	return time.Since(st), err
}

// Story ...
func (w *Workload) Story() (time.Duration, error) {
	st := time.Now()

	var storyID int64
	for storyID == 0 {
		storyID = w.storyVoteSampler.Sample()
	}
	_, err := w.ops.Story(idToShortID(storyID))

	return time.Since(st), err
}

// AddUser ...
func (w *Workload) AddUser() error {
	return w.ops.AddUser()
}

// Submit ...
func (w *Workload) Submit(id int64) (time.Duration, error) {
	st := time.Now()

	err := w.ops.Submit(1, idToShortID(id), fmt.Sprintf("story %d", id))

	return time.Since(st), err
}

// Comment ...
func (w *Workload) Comment() (time.Duration, error) {
	st := time.Now()
	var storyID int64
	for storyID == 0 {
		storyID = w.commentStorySampler.Sample()
	}
	err := w.ops.Comment(1, storyID)

	return time.Since(st), err

}

// StoryVote ...
func (w *Workload) StoryVote(vote int) (time.Duration, error) {
	st := time.Now()

	var storyID int64
	for storyID == 0 {
		storyID = w.storyVoteSampler.Sample()
	}
	err := w.ops.StoryVote(1, storyID, vote)

	return time.Since(st), err
}

// CommentVote ...
func (w *Workload) CommentVote(vote int) (time.Duration, error) {
	st := time.Now()

	var commentID int64
	for commentID == 0 {
		commentID = w.commentVoteSampler.Sample()
	}
	err := w.ops.CommentVote(1, commentID, vote)

	return time.Since(st), err
}

// Close ...
func (w Workload) Close() {
	w.ops.Close()
}

func idToShortID(id int64) string {
	str := make([]rune, 6)

	digit := id % 36
	if digit < 10 {
		str[5] = rune(digit) + '0'
	} else {
		str[5] = rune(digit) - 10 + 'a'
	}

	id /= 36
	digit = id % 36
	if digit < 10 {
		str[4] = rune(digit) + '0'
	} else {
		str[4] = rune(digit) - 10 + 'a'
	}

	id /= 36
	digit = id % 36
	if digit < 10 {
		str[3] = rune(digit) + '0'
	} else {
		str[3] = rune(digit) - 10 + 'a'
	}

	id /= 36
	digit = id % 36
	if digit < 10 {
		str[2] = rune(digit) + '0'
	} else {
		str[2] = rune(digit) - 10 + 'a'
	}

	id /= 36
	digit = id % 36
	if digit < 10 {
		str[1] = rune(digit) + '0'
	} else {
		str[1] = rune(digit) - 10 + 'a'
	}

	id /= 36
	digit = id % 36
	if digit < 10 {
		str[0] = rune(digit) + '0'
	} else {
		str[0] = rune(digit) - 10 + 'a'
	}

	return string(str)
}

func er(err error) {
	debug.PrintStack()
	log.Fatal(err)
}

var votesPerStory = []distributions.Distribution{
	distributions.Distribution{
		Bin:   0,
		Count: 411,
	},
	distributions.Distribution{
		Bin:   10,
		Count: 403,
	},
	distributions.Distribution{
		Bin:   20,
		Count: 113,
	},
	distributions.Distribution{
		Bin:   30,
		Count: 42,
	},
	distributions.Distribution{
		Bin:   40,
		Count: 17,
	},
	distributions.Distribution{
		Bin:   50,
		Count: 7,
	},
	distributions.Distribution{
		Bin:   60,
		Count: 4,
	},
	distributions.Distribution{
		Bin:   70,
		Count: 2,
	},
	distributions.Distribution{
		Bin:   80,
		Count: 1,
	},
}

var votesPerComment = []distributions.Distribution{
	distributions.Distribution{
		Bin:   0,
		Count: 741,
	},
	distributions.Distribution{
		Bin:   10,
		Count: 228,
	},
	distributions.Distribution{
		Bin:   20,
		Count: 23,
	},
	distributions.Distribution{
		Bin:   30,
		Count: 5,
	},
	distributions.Distribution{
		Bin:   40,
		Count: 2,
	},
	distributions.Distribution{
		Bin:   50,
		Count: 1,
	},
}

var commentsPerStory = []distributions.Distribution{
	distributions.Distribution{
		Bin:   0,
		Count: 836,
	},
	distributions.Distribution{
		Bin:   10,
		Count: 119,
	},
	distributions.Distribution{
		Bin:   20,
		Count: 25,
	},
	distributions.Distribution{
		Bin:   30,
		Count: 10,
	},
	distributions.Distribution{
		Bin:   40,
		Count: 5,
	},
	distributions.Distribution{
		Bin:   50,
		Count: 3,
	},
	distributions.Distribution{
		Bin:   60,
		Count: 1,
	},
	distributions.Distribution{
		Bin:   70,
		Count: 1,
	},
}
