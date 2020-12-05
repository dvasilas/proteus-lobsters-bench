package operations

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	"github.com/dvasilas/proteus-lobsters-bench/internal/datastore"
	"github.com/dvasilas/proteus-lobsters-bench/internal/distributions"
	"github.com/dvasilas/proteus-lobsters-bench/internal/measurements"
	queryengine "github.com/dvasilas/proteus-lobsters-bench/internal/query-engine"
	"github.com/dvasilas/proteus/pkg/proteus-go-client/pb"
	"github.com/go-sql-driver/mysql"
)

// Operations ...
type Operations struct {
	config              *config.BenchmarkConfig
	qe                  queryengine.QueryEngine
	ds                  datastore.Datastore
	storyVoteSampler    distributions.Sampler
	commentVoteSampler  distributions.Sampler
	commentStorySampler distributions.Sampler
	StoryID             int64
	topStories          []int64
	voteDistribution    config.DistributionType
}

// Operation ...
type Operation interface {
	DoOperation() (measurements.OpType, time.Duration, time.Time)
}

// NewOperations ...
func NewOperations(conf *config.BenchmarkConfig) (*Operations, error) {
	rand.Seed(time.Now().UTC().UnixNano())
	var ds datastore.Datastore
	var qe queryengine.QueryEngine
	var err error
	ds, err = datastore.NewDatastore(conf.Connection.DBEndpoint, conf.Connection.Database, conf.Connection.AccessKeyID, conf.Connection.SecretAccessKey)
	if err != nil {
		return nil, err
	}

	if !conf.Benchmark.DoPreload && conf.Operations.WriteRatio < 1.0 {
		switch conf.Benchmark.MeasuredSystem {
		case "proteus":
			qe, err = queryengine.NewProteusQE(conf.Connection.ProteusEndpoint, conf.Connection.PoolSize, conf.Connection.PoolOverflow, conf.Tracing)
			if err != nil {
				return nil, err
			}
		case "mysql":
			qe, err = queryengine.NewMysqlQE(conf.Connection.ProteusEndpoint, conf.Connection.PoolSize, conf.Connection.PoolOverflow, conf.Tracing)
			if err != nil {
				return nil, err
			}
		case "baseline":
			qe = queryengine.NewBaselineQE(&ds)
		default:
			return nil, errors.New("invalid 'system' argument")
		}
	}

	ops := &Operations{
		config:              conf,
		qe:                  qe,
		ds:                  ds,
		storyVoteSampler:    distributions.NewSampler(conf.Distributions.VotesPerStory),
		commentVoteSampler:  distributions.NewSampler(conf.Distributions.VotesPerComment),
		commentStorySampler: distributions.NewSampler(conf.Distributions.CommentsPerStory),
		StoryID:             conf.Preload.RecordCount.Stories,
	}

	switch conf.Operations.DistributionType {
	case "uniform":
		ops.voteDistribution = config.Uniform
	case "histogram":
		ops.voteDistribution = config.Histogram
	case "voteTopStories":
		ops.voteDistribution = config.VoteTopStories
	default:
		return nil, errors.New("unexpected distribution type")
	}

	if ops.voteDistribution == config.VoteTopStories {
		topStories, err := ops.getTopStories()
		if err != nil {
			return nil, err
		}
		ops.topStories = topStories
	}

	return ops, nil
}

// StoryVote ...
type StoryVote struct {
	Ops  *Operations
	Vote int
}

// DoOperation ...
func (op StoryVote) DoOperation() (measurements.OpType, time.Duration, time.Time) {
	respTime, err := op.Ops.StoryVote(op.Vote)
	if err != nil {
		if strings.Contains(err.Error(), "Deadlock") {
			return measurements.Deadlock, respTime, time.Now()
		} else if strings.Contains(err.Error(), "out of sync") || strings.Contains(err.Error(), "bad connection") || err == mysql.ErrInvalidConn {
			// er(err)
			return measurements.Deadlock, respTime, time.Now()
		}
	}
	return measurements.Write, respTime, time.Now()
}

// StoryVote issues an up or down vote for the given story.
func (op *Operations) StoryVote(vote int) (time.Duration, error) {
	var storyID int64
	var err error
	for storyID == 0 {
		switch op.voteDistribution {
		case config.VoteTopStories:
			storyID = op.topStories[rand.Intn(len(op.topStories))]
		case config.Histogram:
			storyID = op.storyVoteSampler.Sample()
		case config.Uniform:
			storyID = rand.Int63n(op.config.Preload.RecordCount.Stories)
		}
	}
	st := time.Now()
	if op.config.Benchmark.MeasuredSystem == "proteus" {
		err = op.ds.StoryVoteSimple(1, storyID, vote)
	} else if op.config.Benchmark.MeasuredSystem == "proteus" {
		err = op.qe.StoryVote(storyID, vote)
	} else if op.config.Benchmark.MeasuredSystem == "baseline" {
		err = op.ds.StoryVoteUpdateCount(1, storyID, vote)
	}
	return time.Since(st), err
}

// CommentVote ...
type CommentVote struct {
	Ops  *Operations
	Vote int
}

// DoOperation ...
func (op CommentVote) DoOperation() (measurements.OpType, time.Duration, time.Time) {
	respTime, err := op.Ops.CommentVote(op.Vote)
	if err != nil {
		er(err)
	}
	return measurements.Write, respTime, time.Now()
}

// CommentVote issues an up or down vote for the given comment.
func (op *Operations) CommentVote(vote int) (time.Duration, error) {
	return 0, errors.New("not implemented")
}

// Frontpage ...
type Frontpage struct {
	Ops *Operations
}

// DoOperation ...
func (op Frontpage) DoOperation() (measurements.OpType, time.Duration, time.Time) {
	respTime, err := op.Ops.Frontpage()
	if err != nil {
		if strings.Contains(err.Error(), "out of sync") || strings.Contains(err.Error(), "bad connection") || err == mysql.ErrInvalidConn {
			// er(err)
			return measurements.Deadlock, respTime, time.Now()
		}
	}
	return measurements.Read, respTime, time.Now()
}

// GetTopStories ...
func (op *Operations) getTopStories() ([]int64, error) {
	topStories := make([]int64, op.config.Operations.Homepage.StoriesLimit)
	queryStr := fmt.Sprintf("SELECT title, description, short_id, user_id, vote_sum FROM stories ORDER BY vote_sum DESC LIMIT %d",
		op.config.Operations.Homepage.StoriesLimit)

	resp, err := op.qe.Query(queryStr)
	if err != nil {
		return topStories, err
	}

	// hp := Homepage{}
	switch op.config.Benchmark.MeasuredSystem {
	case "proteus":
		response := resp.(*pb.QueryResp)
		for i, entry := range response.GetRespRecord() {
			sID, err := strconv.ParseInt(entry.GetAttributes()["story_id"], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			topStories[i] = sID
		}
	}

	return topStories, nil
}

// Frontpage renders the frontpage (https://lobste.rs/).
func (op *Operations) Frontpage() (time.Duration, error) {
	queryStr := fmt.Sprintf("SELECT title, description, short_id, user_id, vote_sum FROM stories ORDER BY vote_sum DESC LIMIT %d",
		op.config.Operations.Homepage.StoriesLimit)

	var duration time.Duration
	st := time.Now()
	_, err := op.qe.Query(queryStr)
	duration = time.Since(st)
	if err != nil {
		return duration, err
	}

	// hp := Homepage{}
	switch op.config.Benchmark.MeasuredSystem {
	case "proteus":
		// response := resp.(*pb.QueryResp)
		// for _, entry := range response.GetRespRecord() {
		// 	fmt.Println(entry.GetAttributes()["title"], entry.GetAttributes()["short_id"], entry.GetAttributes()["vote_sum"])
		// }
		// fmt.Println()
	}

	return duration, nil
}

// Story ...
type Story struct {
	Ops *Operations
}

// DoOperation ...
func (op Story) DoOperation() (measurements.OpType, time.Duration, time.Time) {
	respTime, err := op.Ops.Story()
	if err != nil {
		er(err)
	}
	return measurements.Read, respTime, time.Now()
}

// Story renders a particular stor based a given shortID (https://lobste.rs/s/cqnzl5/).
func (op *Operations) Story() (time.Duration, error) {
	var storyID int64
	for storyID == 0 {
		storyID = op.storyVoteSampler.Sample()
	}
	shortID := idToShortID(storyID)

	queryStr := fmt.Sprintf("SELECT title, description, short_id, user_id, vote_sum FROM stories WHERE short_id = '%s'", shortID)

	var duration time.Duration
	st := time.Now()
	_, err := op.qe.Query(queryStr)
	duration = time.Since(st)
	if err != nil {
		return duration, err
	}

	switch op.config.Benchmark.MeasuredSystem {
	case "proteus":
	case "mysql_plain":
	case "mysql_mv":
	}

	return duration, nil
}

// Comment ...
type Comment struct {
	Ops *Operations
}

// DoOperation ...
func (op Comment) DoOperation() (measurements.OpType, time.Duration, time.Time) {
	respTime, err := op.Ops.Comment()
	if err != nil {
		er(err)
	}
	return measurements.Write, respTime, time.Now()
}

// Comment ...
func (op *Operations) Comment() (time.Duration, error) {
	var storyID int64
	for storyID == 0 {
		storyID = op.commentStorySampler.Sample()
	}

	var duration time.Duration
	comment, err := randString(20)
	if err != nil {
		return duration, err
	}

	st := time.Now()
	err = op.ds.Comment(1, storyID, comment)
	return time.Since(st), err
}

// Submit ...
type Submit struct {
	Ops *Operations
}

// DoOperation ...
func (op Submit) DoOperation() (measurements.OpType, time.Duration, time.Time) {
	respTime, err := op.Ops.Submit()
	if err != nil {
		er(err)
	}
	return measurements.Write, respTime, time.Now()
}

// Submit a new story to the site.
func (op *Operations) Submit() (time.Duration, error) {
	var duration time.Duration

	id := atomic.AddInt64(&op.StoryID, 1)

	description, err := randString(30)
	if err != nil {
		return duration, err
	}

	st := time.Now()
	err = op.ds.Submit(1, fmt.Sprintf("story %d", id), description, idToShortID(id))
	return time.Since(st), err
}

// AddUser ...
func (op *Operations) AddUser() error {
	userName, err := randString(10)
	if err != nil {
		return err
	}
	return op.ds.Adduser(userName[:10])
}

// Recent renders recently submitted stories (https://lobste.rs/recent).
func (op *Operations) recent() {}

// Comments renders recently submitted comments (https://lobste.rs/recent).
func (op *Operations) comments() {}

// User renders a user's profile(https://lobste.rs/u/jonhoo).
func (op *Operations) user(username string) {}

// Login logs in a user.
func (op *Operations) login() {}

// Logout logs out a user.
func (op *Operations) logout() {}

// Close ...
func (op *Operations) Close() {
	if op.qe != nil {
		op.qe.Close()
	}
}

func randString(length int) (string, error) {
	b, err := generateRandomBytes(length)
	return base64.URLEncoding.EncodeToString(b), err
}

func generateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return nil, err
	}

	return b, nil
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
	fmt.Println(err)
	debug.PrintStack()
	log.Fatal(err)
}
