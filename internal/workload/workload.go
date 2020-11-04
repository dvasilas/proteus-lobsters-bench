package workload

import (
	"fmt"
	"log"
	"math/rand"
	"runtime/debug"
	"strings"
	"sync"

	"time"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	"github.com/dvasilas/proteus-lobsters-bench/internal/operations"
	"github.com/dvasilas/proteus-lobsters-bench/internal/perf"
)

// Type ...
type Type int

const (
	// Simple ...
	Simple Type = iota
	// Complete ...
	Complete Type = iota
)

// Workload ...
type Workload struct {
	config       *config.BenchmarkConfig
	ops          *operations.Operations
	measurements *perf.Perf
}

// NewWorkload ...
func NewWorkload(conf *config.BenchmarkConfig) (*Workload, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	ops, err := operations.NewOperations(conf)
	if err != nil {
		return nil, err
	}

	return &Workload{
		config:       conf,
		ops:          ops,
		measurements: perf.New(),
	}, nil
}

type measurement struct {
	respTime time.Duration
	opType   OpType
	endTs    time.Time
}

func doOperationAsync(op Operation, measurementsCh chan measurement, pendingOperations *int64, limitReadCh, limitWriteCh chan struct{}) {
	switch op.(type) {
	case Frontpage, Story:
		defer func() { <-limitReadCh }()
	case StoryVote, CommentVote, Submit, Comment:
		defer func() { <-limitWriteCh }()
	}

	opType, respTime, endTs := op.DoOperation()

	measurementsCh <- measurement{
		respTime: respTime,
		opType:   opType,
		endTs:    endTs,
	}
}

func measurementsConsumer(measurementsCh chan measurement, measurementBuf *[]measurement, deadlockAborts *int64, doneCh chan bool, warmupEnd, end time.Time) {
	for i, t := 0, time.NewTimer(2*time.Second); true; i++ {
		select {
		case m, isopen := <-measurementsCh:
			if !isopen {
				return
			}
			if m.opType == Deadlock {
				*deadlockAborts++
			} else {
				if m.endTs.UnixNano() > warmupEnd.UnixNano() && m.endTs.UnixNano() < end.UnixNano() {
					*measurementBuf = append(*measurementBuf, m)
				}
			}
			t.Reset(2 * time.Second)
		case <-t.C:
			close(doneCh)
			return
		}
	}

}

// Client ...
func (w Workload) Client(workloadType Type, measurementBufferSize int64) (time.Duration, int64, map[string][]time.Duration, int64) {
	target := w.config.Benchmark.TargetLoad
	interArrival := time.Duration(1e9/float64(target)) * time.Nanosecond

	measurementsCh := make(chan measurement)
	var pending int64
	measurementBuff := make([]measurement, 0)
	doneCh := make(chan bool)

	deadlockAborts := int64(0)

	var totalOpCnt, opCnt int64
	var op Operation
	var st, now, next time.Time

	st = time.Now()
	end := st.Add(time.Duration(w.config.Benchmark.Runtime) * time.Second)

	warmpupEnd := st.Add(time.Duration(w.config.Benchmark.Warmup) * time.Second)
	warmupShrortCirc := true

	limitReadCh := make(chan struct{}, w.config.Benchmark.MaxInFlightRead)
	limitWriteCh := make(chan struct{}, w.config.Benchmark.MaxInFlightWrite)

	go measurementsConsumer(measurementsCh, &measurementBuff, &deadlockAborts, doneCh, warmpupEnd, end)

	nextOp := true
	next = time.Now()

	for time.Now().UnixNano() < end.UnixNano() {
		if warmupShrortCirc && time.Now().UnixNano() > warmpupEnd.UnixNano() {
			warmupShrortCirc = false
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

		if nextOp {
			nextOp = false
			switch workloadType {
			case Simple:
				op = w.NextOpSimple()
			case Complete:
				op = w.NextOpComplete()
			}
		}

		switch op.(type) {
		case Frontpage, Story:
			select {
			case limitReadCh <- struct{}{}:
				nextOp = true
			default:
				continue
			}
		case StoryVote, CommentVote, Submit, Comment:
			select {
			case limitWriteCh <- struct{}{}:
				nextOp = true
			default:
				continue
			}
		}

		go doOperationAsync(op, measurementsCh, &pending, limitReadCh, limitWriteCh)

		opCnt++
		totalOpCnt++

		next = next.Add(interArrival)
	}
	en := time.Now()
	runtime := en.Sub(st)

	<-doneCh

	durations := make(map[string][]time.Duration)
	durations["read"] = make([]time.Duration, 0)
	durations["write"] = make([]time.Duration, 0)

	for _, m := range measurementBuff {
		if m.opType == Write {
			durations["write"] = append(durations["write"], m.respTime)
		} else {
			durations["read"] = append(durations["read"], m.respTime)
		}
	}

	return runtime, opCnt, durations, deadlockAborts
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

// Operation ...
type Operation interface {
	DoOperation() (OpType, time.Duration, time.Time)
}

// StoryVote ...
type StoryVote struct {
	ops  *operations.Operations
	vote int
}

// DoOperation ...
func (op StoryVote) DoOperation() (OpType, time.Duration, time.Time) {
	respTime, err := op.ops.StoryVote(op.vote)
	if err != nil {
		if strings.Contains(err.Error(), "Deadlock") {
			return Deadlock, respTime, time.Now()
		}
		er(err)
	}
	return Write, respTime, time.Now()
}

// CommentVote ...
type CommentVote struct {
	ops  *operations.Operations
	vote int
}

// DoOperation ...
func (op CommentVote) DoOperation() (OpType, time.Duration, time.Time) {
	respTime, err := op.ops.CommentVote(op.vote)
	if err != nil {
		er(err)
	}
	return Write, respTime, time.Now()
}

// Frontpage ...
type Frontpage struct {
	ops *operations.Operations
}

// DoOperation ...
func (op Frontpage) DoOperation() (OpType, time.Duration, time.Time) {
	respTime, err := op.ops.Frontpage()
	if err != nil {
		er(err)
	}
	return Read, respTime, time.Now()
}

// Story ...
type Story struct {
	ops *operations.Operations
}

// DoOperation ...
func (op Story) DoOperation() (OpType, time.Duration, time.Time) {
	respTime, err := op.ops.Story()
	if err != nil {
		er(err)
	}
	return Read, respTime, time.Now()
}

// Comment ...
type Comment struct {
	ops *operations.Operations
}

// DoOperation ...
func (op Comment) DoOperation() (OpType, time.Duration, time.Time) {
	respTime, err := op.ops.Comment()
	if err != nil {
		er(err)
	}
	return Write, respTime, time.Now()
}

// Submit ...
type Submit struct {
	ops *operations.Operations
}

// DoOperation ...
func (op Submit) DoOperation() (OpType, time.Duration, time.Time) {
	respTime, err := op.ops.Submit()
	if err != nil {
		er(err)
	}
	return Write, respTime, time.Now()
}

// NextOpSimple ...
func (w Workload) NextOpSimple() Operation {
	r := rand.Float64()

	if r < w.config.Operations.WriteRatio {
		vote := rand.Float64()
		if vote < w.config.Operations.DownVoteRatio {
			return StoryVote{ops: w.ops, vote: -1}
		}
		return StoryVote{ops: w.ops, vote: 1}
	}

	return Frontpage{ops: w.ops}
}

// NextOpComplete ...
func (w Workload) NextOpComplete() Operation {
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
			return Story{ops: w.ops}
		} else if applies(30105, &seed) {
			// /
			return Frontpage{ops: w.ops}
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
			return CommentVote{ops: w.ops, vote: 1}
		} else if applies(475, &seed) {
			// /stories/X/upvote
			return StoryVote{ops: w.ops, vote: 1}
		} else if applies(316, &seed) {
			// /comments
			return Comment{ops: w.ops}
		} else if applies(87, &seed) {
			// /login
			continue
		} else if applies(71, &seed) {
			// /comments/X
			continue
		} else if applies(54, &seed) {
			// /comments/X/downvote
			return CommentVote{ops: w.ops, vote: -1}
		} else if applies(53, &seed) {
			// /stories
			return Submit{ops: w.ops}
		} else if applies(21, &seed) {
			// /stories/X/downvote
			return StoryVote{ops: w.ops, vote: -1}
		} else {
			// /logout
			continue
		}
	}
	return Frontpage{}
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

func er(err error) {
	fmt.Println(err)
	debug.PrintStack()
	log.Fatal(err)
}
