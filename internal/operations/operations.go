package operations

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	"github.com/dvasilas/proteus-lobsters-bench/internal/datastore"
	"github.com/dvasilas/proteus-lobsters-bench/internal/distributions"
	queryengine "github.com/dvasilas/proteus-lobsters-bench/internal/query-engine"
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

// Homepage ...
type Homepage struct {
	Stories []Story
}

// Story ...
type Story struct {
	StoryID     int64
	Title       string
	Description string
	ShortID     string
	VoteCount   int64
}

// NewOperations ...
func NewOperations(conf *config.BenchmarkConfig) (*Operations, error) {
	ds, err := datastore.NewDatastore(conf.Connection.DBEndpoint, conf.Connection.Database, conf.Connection.AccessKeyID, conf.Connection.SecretAccessKey)
	if err != nil {
		return nil, err
	}

	var qe queryengine.QueryEngine
	if !conf.Benchmark.DoPreload {
		switch conf.Benchmark.MeasuredSystem {
		case "proteus":
			qe, err = queryengine.NewProteusQueryEngine(conf.Connection.ProteusEndpoint, conf.Connection.PoolSize, conf.Connection.PoolOverflow, conf.Tracing)
			if err != nil {
				return nil, err
			}
		case "mysql_plain":
			qe = queryengine.NewMySQLPlainQE(&ds)
		case "mysql_mv":
			qe = queryengine.NewMySQLWithViewsQE(&ds)
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

// Frontpage renders the frontpage (https://lobste.rs/).
func (op *Operations) Frontpage() (time.Duration, error) {
	queryStr := fmt.Sprintf("SELECT title, description, short_id, user_id, vote_count FROM stories ORDER BY vote_count DESC LIMIT %d",
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
		// stories := make([]Story, len(response.GetRespRecord()))
		// for i, entry := range response.GetRespRecord() {
		// 	stories[i] = Story{
		// 		Title:   string(entry.GetAttributes()["title"]),
		// 		ShortID: string(entry.GetAttributes()["short_id"]),
		// 	}

		// 	val, err := strconv.ParseInt(entry.GetRecordId(), 10, 64)
		// 	if err != nil {
		// 		return Homepage{}, err
		// 	}
		// 	stories[i].StoryID = val

		// 	if _, ok := entry.GetAttributes()["vote_count"]; ok {
		// 		val, err := strconv.ParseInt(string(entry.GetAttributes()["vote_count"]), 10, 64)
		// 		if err != nil {
		// 			return Homepage{}, err
		// 		}
		// 		stories[i].VoteCount = val
		// 	}
		// }
		// hp.Stories = stories
	case "mysql_plain":
		// response := resp.([]map[string]string)
		// stories := make([]Story, len(response))
		// for i, entry := range response {
		// 	stories[i] = Story{
		// 		Title:       entry["title"],
		// 		Description: entry["description"],
		// 		ShortID:     entry["short_id"],
		// 	}

		// 	val, err := strconv.ParseInt(entry["vote_count"], 10, 64)
		// 	if err != nil {
		// 		return duration, Homepage{}, err
		// 	}
		// 	stories[i].VoteCount = val
		// }
		// hp.Stories = stories
	case "mysql_mv":
		// response := resp.([]map[string]string)
		// stories := make([]Story, len(response))
		// for i, entry := range response {
		// 	stories[i] = Story{
		// 		Title:       entry["title"],
		// 		Description: entry["description"],
		// 		ShortID:     entry["short_id"],
		// 	}

		// 	val, err := strconv.ParseInt(entry["vote_count"], 10, 64)
		// 	if err != nil {
		// 		return duration, Homepage{}, err
		// 	}
		// 	stories[i].VoteCount = val
		// }
		// hp.Stories = stories
	}

	return duration, nil
}

// Recent renders recently submitted stories (https://lobste.rs/recent).
func (op *Operations) Recent() {}

// Comments renders recently submitted comments (https://lobste.rs/recent).
func (op *Operations) Comments() {}

// User renders a user's profile(https://lobste.rs/u/jonhoo).
func (op *Operations) User(username string) {}

// Login logs in a user.
func (op *Operations) Login() {}

// Logout logs out a user.
func (op *Operations) Logout() {}

// Story renders a particular stor based a given shortID (https://lobste.rs/s/cqnzl5/).
func (op *Operations) Story() (time.Duration, error) {
	var storyID int64
	for storyID == 0 {
		storyID = op.storyVoteSampler.Sample()
	}
	shortID := idToShortID(storyID)

	queryStr := fmt.Sprintf("SELECT title, description, short_id, user_id, vote_count FROM stories WHERE short_id = '%s'", shortID)

	var duration time.Duration
	st := time.Now()
	_, err := op.qe.Query(queryStr)
	duration = time.Since(st)
	if err != nil {
		return duration, err
	}

	switch op.config.Benchmark.MeasuredSystem {
	case "proteus":
		// response := resp.(*pb.QueryResp)
		// for _, entry := range response.GetRespRecord() {
		// 	story.Title = string(entry.GetAttributes()["title"])
		// 	story.ShortID = string(entry.GetAttributes()["short_id"])

		// 	val, err := strconv.ParseInt(entry.GetRecordId(), 10, 64)
		// 	if err != nil {
		// 		return Story{}, err
		// 	}
		// 	story.StoryID = val

		// 	if _, ok := entry.GetAttributes()["vote_count"]; ok {
		// 		val, err := strconv.ParseInt(string(entry.GetAttributes()["vote_count"]), 10, 64)
		// 		if err != nil {
		// 			return Story{}, err
		// 		}
		// 		story.VoteCount = val
		// 	}
		// }

	case "mysql_plain":
	case "mysql_mv":
	}

	return duration, nil
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

// CommentVote issues an up or down vote for the given comment.
func (op *Operations) CommentVote(vote int) (time.Duration, error) {
	var commentID int64
	for commentID == 0 {
		commentID = op.commentVoteSampler.Sample()
	}

	var duration time.Duration

	st := time.Now()
	_, err := op.ds.Get("comments", "story_id", map[string]interface{}{"id": commentID})
	if err != nil {
		return duration, err
	}

	//err = op.ds.Insert(
	//	"votes",
	//	map[string]interface{}{
	//		"user_id":    1,
	//		"story_id":   storyID,
	//		"comment_id": commentID,
	//		"vote":       vote,
	//	})
	return time.Since(st), err
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

// AddUser ...
func (op *Operations) AddUser() error {
	userName, err := randString(10)
	if err != nil {
		return err
	}
	return op.ds.Adduser(userName[:10])
}

// Close ...
func (op *Operations) Close() {
	op.qe.Close()
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
