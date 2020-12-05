package queryengine

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	proteusclient "github.com/dvasilas/proteus/pkg/proteus-go-client"
)

// QueryEngine ...
type QueryEngine interface {
	Query(query string) (interface{}, error)
	StoryVote(storyID int64, vote int) error
	Close()
}

// ProteusQE ...
type ProteusQE struct {
	proteusClient *proteusclient.Client
}

// MysqlQE ...
type MysqlQE struct {
	proteusClient *proteusclient.Client
}

// --------------------- Proteus query engine --------------------

// NewProteusQE ...
func NewProteusQE(endpoint string, poolSize, poolOverflow int, tracing bool) (ProteusQE, error) {
	for {
		c, err := net.DialTimeout("tcp", endpoint, time.Duration(time.Second))
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retrying connecting to: ", endpoint)
		} else {
			c.Close()
			break
		}
	}

	port, err := strconv.ParseInt(strings.Split(endpoint, ":")[1], 10, 64)
	if err != nil {
		return ProteusQE{}, err
	}
	c, err := proteusclient.NewClient(proteusclient.Host{Name: strings.Split(endpoint, ":")[0], Port: int(port)}, poolSize, poolOverflow, tracing)
	if err != nil {
		return ProteusQE{}, err
	}

	err = errors.New("not tried yet")
	for err != nil {
		_, err = c.Query("SELECT title, description, short_id, user_id, vote_sum FROM stories ORDER BY vote_sum DESC LIMIT 2")
		if err != nil {
			return ProteusQE{}, err
		}
		time.Sleep(2 * time.Second)
		fmt.Println("retrying a test query", err)
	}

	return ProteusQE{
		proteusClient: c,
	}, nil
}

// Query ...
func (qe ProteusQE) Query(query string) (resp interface{}, err error) {
	resp, err = qe.proteusClient.Query(query)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// StoryVote ...
func (qe ProteusQE) StoryVote(storyID int64, vote int) error {
	return nil
}

// Close ...
func (qe ProteusQE) Close() {
	qe.proteusClient.Close()
}

// ------------------ MySQL query engine ---------------

// NewMysqlQE ...
func NewMysqlQE(endpoint string, poolSize, poolOverflow int, tracing bool) (MysqlQE, error) {
	for {
		c, err := net.DialTimeout("tcp", endpoint, time.Duration(time.Second))
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retrying connecting to: ", endpoint)
		} else {
			c.Close()
			break
		}
	}

	port, err := strconv.ParseInt(strings.Split(endpoint, ":")[1], 10, 64)
	if err != nil {
		return MysqlQE{}, err
	}
	c, err := proteusclient.NewClient(proteusclient.Host{Name: strings.Split(endpoint, ":")[0], Port: int(port)}, poolSize, poolOverflow, tracing)
	if err != nil {
		return MysqlQE{}, err
	}

	err = errors.New("not tried yet")
	for err != nil {
		_, err = c.LobstersFrontpage()
		if err != nil {
			return MysqlQE{}, err
		}
		time.Sleep(2 * time.Second)
		fmt.Println("retrying a test query", err)
	}

	return MysqlQE{
		proteusClient: c,
	}, nil
}

// Query ...
func (qe MysqlQE) Query(query string) (interface{}, error) {
	return qe.proteusClient.LobstersFrontpage()
}

// StoryVote ...
func (qe MysqlQE) StoryVote(storyID int64, vote int) error {
	_, err := qe.proteusClient.LobstersStoryVote(storyID, vote)
	return err
}

// Close ...
func (qe MysqlQE) Close() {
	qe.proteusClient.Close()
}
