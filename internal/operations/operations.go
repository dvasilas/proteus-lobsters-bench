package operations

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dvasilas/proteus-lobsters-bench/internal/config"
	"github.com/dvasilas/proteus-lobsters-bench/internal/datastore"
	queryengine "github.com/dvasilas/proteus-lobsters-bench/internal/query-engine"
)

// Operations ...
type Operations struct {
	config *config.BenchmarkConfig
	qe     queryengine.QueryEngine
	ds     datastore.Datastore
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
			qe, err = queryengine.NewProteusQueryEngine(conf.Connection.ProteusEndpoint, conf.Tracing)
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
		config: conf,
		qe:     qe,
		ds:     ds,
	}, nil
}

// Frontpage renders the frontpage (https://lobste.rs/).
func (op *Operations) Frontpage() (Homepage, error) {
	queryStr := fmt.Sprintf("SELECT title, description, short_id, user_id, vote_sum FROM stories ORDER BY vote_sum DESC LIMIT %d",
		op.config.Operations.Homepage.StoriesLimit)

	resp, err := op.qe.Query(queryStr)
	if err != nil {
		return Homepage{}, err
	}
	hp := Homepage{}
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

		// 	if _, ok := entry.GetAttributes()["vote_sum"]; ok {
		// 		val, err := strconv.ParseInt(string(entry.GetAttributes()["vote_sum"]), 10, 64)
		// 		if err != nil {
		// 			return Homepage{}, err
		// 		}
		// 		stories[i].VoteCount = val
		// 	}
		// }
		// hp.Stories = stories
	case "mysql_plain":
		response := resp.([]map[string]string)
		stories := make([]Story, len(response))
		for i, entry := range response {
			stories[i] = Story{
				Title:       entry["title"],
				Description: entry["description"],
				ShortID:     entry["short_id"],
			}

			val, err := strconv.ParseInt(entry["vote_count"], 10, 64)
			if err != nil {
				return Homepage{}, err
			}
			stories[i].VoteCount = val
		}
		hp.Stories = stories
	case "mysql_mv":
		response := resp.([]map[string]string)
		stories := make([]Story, len(response))
		for i, entry := range response {
			stories[i] = Story{
				Title:       entry["title"],
				Description: entry["description"],
				ShortID:     entry["short_id"],
			}

			val, err := strconv.ParseInt(entry["vote_count"], 10, 64)
			if err != nil {
				return Homepage{}, err
			}
			stories[i].VoteCount = val
		}
		hp.Stories = stories
	}

	return hp, nil
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
func (op *Operations) Story(shortID string) (Story, error) {
	queryStr := fmt.Sprintf("SELECT title, description, short_id, user_id, vote_sum FROM stories WHERE short_id = '%s'", shortID)

	_, err := op.qe.Query(queryStr)
	if err != nil {
		return Story{}, err
	}

	story := Story{}
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

		// 	if _, ok := entry.GetAttributes()["vote_sum"]; ok {
		// 		val, err := strconv.ParseInt(string(entry.GetAttributes()["vote_sum"]), 10, 64)
		// 		if err != nil {
		// 			return Story{}, err
		// 		}
		// 		story.VoteCount = val
		// 	}
		// }

	case "mysql_plain":
	case "mysql_mv":
	}

	return story, nil
}

// StoryVote issues an up or down vote for the given story.
func (op *Operations) StoryVote(userID, storyID int64, vote int) error {
	return op.ds.Insert(
		"votes",
		map[string]interface{}{
			"user_id":  userID,
			"story_id": storyID,
			"vote":     vote,
		})
}

// CommentVote issues an up or down vote for the given comment.
func (op *Operations) CommentVote(userID, commentID int64, vote int) error {
	storyID, err := op.ds.Get("comments", "story_id", map[string]interface{}{"id": commentID})
	if err != nil {
		return err
	}

	return op.ds.Insert(
		"votes",
		map[string]interface{}{
			"user_id":    userID,
			"story_id":   storyID,
			"comment_id": commentID,
			"vote":       vote,
		})
}

// Submit a new story to the site.
func (op *Operations) Submit(userID int64, shortID, title string) error {
	description, err := randString(30)
	if err != nil {
		return err
	}

	if err := op.ds.Insert(
		"stories",
		map[string]interface{}{
			"user_id":     userID,
			"title":       title,
			"description": description,
			"short_id":    shortID,
		},
	); err != nil {
		return err
	}

	return nil
}

// Comment ...
func (op *Operations) Comment(userID, storyID int64) error {
	comment, err := randString(20)
	if err != nil {
		return err
	}

	if err := op.ds.Insert(
		"comments",
		map[string]interface{}{
			"user_id":  userID,
			"story_id": storyID,
			"comment":  comment,
		},
	); err != nil {
		return err
	}

	return nil
}

// AddUser ...
func (op *Operations) AddUser() error {
	userName, err := randString(10)
	if err != nil {
		return err
	}
	if err := op.ds.Insert(
		"users",
		map[string]interface{}{"username": userName},
	); err != nil {
		return err
	}

	return nil
}

// Close ...
func (op *Operations) Close() {
	op.qe.Close()
}

// Test ...
func (op *Operations) Test() error {
	fmt.Println("Submit Story ...")
	if err := op.Submit(1, "", ""); err != nil {
		return err
	}

	fmt.Println("GetHomepage ...")
	hp, err := op.Frontpage()
	if err != nil {
		return err
	}
	var storyID int64
	var shortID string
	for _, st := range hp.Stories {
		storyID = st.StoryID
		shortID = st.ShortID
		break
	}

	for _, st := range hp.Stories {
		fmt.Println(st)
	}

	fmt.Println("UpVote story ...")
	if err := op.StoryVote(1, storyID, 1); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)

	fmt.Println("Get Homepage ...")
	hp, err = op.Frontpage()
	if err != nil {
		return err
	}
	for _, st := range hp.Stories {
		fmt.Println(st)
	}

	fmt.Println("Get story by storyID ...")
	story, err := op.Story(shortID)
	if err != nil {
		return err
	}
	fmt.Println(story)

	return nil
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
