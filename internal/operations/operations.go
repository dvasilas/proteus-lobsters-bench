package operations

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	"github.com/dvasilas/proteus-lobsters-bench/internal/datastore"
	"github.com/dvasilas/proteus-lobsters-bench/internal/distributions"
	"github.com/dvasilas/proteus-lobsters-bench/internal/measurements"
	queryengine "github.com/dvasilas/proteus-lobsters-bench/internal/query-engine"
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
}

// Operation ...
type Operation interface {
	DoOperation() (measurements.OpType, time.Duration, time.Time)
}

// NewOperations ...
func NewOperations(conf *config.BenchmarkConfig) (*Operations, error) {
	ds, err := datastore.NewDatastore(conf.Connection.DBEndpoint, conf.Connection.Database, conf.Connection.AccessKeyID, conf.Connection.SecretAccessKey)
	if err != nil {
		return nil, err
	}

	var qe queryengine.QueryEngine
	if !conf.Benchmark.DoPreload && conf.Operations.WriteRatio < 1.0 {
		switch conf.Benchmark.MeasuredSystem {
		case "proteus":
			qe, err = queryengine.NewProteusQE(conf.Connection.ProteusEndpoint, conf.Connection.PoolSize, conf.Connection.PoolOverflow, conf.Tracing)
			if err != nil {
				return nil, err
			}
		case "mysql":
			qe = queryengine.NewMysqlQE(&ds)
		default:
			return nil, errors.New("invalid 'system' argument")
		}
	}

	return &Operations{
		config:              conf,
		qe:                  qe,
		ds:                  ds,
		storyVoteSampler:    distributions.NewSampler(votesPerStory),
		commentVoteSampler:  distributions.NewSampler(votesPerComment),
		commentStorySampler: distributions.NewSampler(commentsPerStory),
		StoryID:             conf.Preload.RecordCount.Stories,
	}, nil
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
		if strings.Contains(err.Error(), "Deadlock") || strings.Contains(err.Error(), "out of sync") || err == mysql.ErrInvalidConn {
			return measurements.Deadlock, respTime, time.Now()
		}
		er(err)
	}
	return measurements.Write, respTime, time.Now()
}

// StoryVote issues an up or down vote for the given story.
func (op *Operations) StoryVote(vote int) (time.Duration, error) {
	var storyID int64
	var err error
	for storyID == 0 {
		storyID = op.storyVoteSampler.Sample()
	}
	st := time.Now()
	if op.config.Benchmark.MeasuredSystem == "proteus" || op.config.Benchmark.MeasuredSystem == "mysql_plain" {
		err = op.ds.StoryVoteSimple(1, storyID, vote)
	} else {
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

	// var commentID int64
	// for commentID == 0 {
	// 	commentID = op.commentVoteSampler.Sample()
	// }

	// var duration time.Duration

	// st := time.Now()
	// _, err := op.ds.Get("comments", "story_id", map[string]interface{}{"id": commentID})
	// if err != nil {
	// 	return duration, err
	// }

	// //err = op.ds.Insert(
	// //	"votes",
	// //	map[string]interface{}{
	// //		"user_id":    1,
	// //		"story_id":   storyID,
	// //		"comment_id": commentID,
	// //		"vote":       vote,
	// //	})
	// return time.Since(st), err
}

// Frontpage ...
type Frontpage struct {
	Ops *Operations
}

// DoOperation ...
func (op Frontpage) DoOperation() (measurements.OpType, time.Duration, time.Time) {
	respTime, err := op.Ops.Frontpage()
	if err != nil {
		er(err)
	}
	return measurements.Read, respTime, time.Now()
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
	case "mysql_plain":
	case "mysql_mv":
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

func er(err error) {
	fmt.Println(err)
	debug.PrintStack()
	log.Fatal(err)
}
