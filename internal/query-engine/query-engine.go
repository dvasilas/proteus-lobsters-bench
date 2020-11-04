package queryengine

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/dvasilas/proteus-lobsters-bench/internal/datastore"
	proteusclient "github.com/dvasilas/proteus/pkg/proteus-go-client"
)

// QueryEngine ...
type QueryEngine interface {
	Query(query string) (interface{}, error)
	Close()
}

// ProteusQueryEngine ...
type ProteusQueryEngine struct {
	proteusClient *proteusclient.Client
}

// --------------------- Proteus --------------------

// NewProteusQueryEngine ...
func NewProteusQueryEngine(endpoint string, poolSize, poolOverflow int, tracing bool) (ProteusQueryEngine, error) {
	for {
		c, err := net.DialTimeout("tcp", endpoint, time.Duration(time.Second))
		if err != nil {
			time.Sleep(2 * time.Second)
			fmt.Println("retying connecting to: ", endpoint)
		} else {
			c.Close()
			break
		}
	}

	port, err := strconv.ParseInt(strings.Split(endpoint, ":")[1], 10, 64)
	if err != nil {
		return ProteusQueryEngine{}, err
	}
	c, err := proteusclient.NewClient(proteusclient.Host{Name: "127.0.0.1", Port: int(port)}, poolSize, poolOverflow, tracing)
	if err != nil {
		return ProteusQueryEngine{}, err
	}

	err = errors.New("not tried yet")
	for err != nil {
		_, err = c.Query("SELECT title, description, short_id, user_id, vote_sum FROM stories ORDER BY vote_sum DESC LIMIT 2")
		time.Sleep(2 * time.Second)
		fmt.Println("retying a test query", err)
	}

	return ProteusQueryEngine{
		proteusClient: c,
	}, nil
}

// Query ...
func (qe ProteusQueryEngine) Query(query string) (resp interface{}, err error) {
	resp, err = qe.proteusClient.Query(query)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Close ...
func (qe ProteusQueryEngine) Close() {
	qe.proteusClient.Close()
}

// ------------------ MySQL (with MVs) ---------------

// MySQLWithViewsQE ...
type MySQLWithViewsQE struct {
	ds *datastore.Datastore
}

// NewMySQLWithViewsQE ...
func NewMySQLWithViewsQE(ds *datastore.Datastore) MySQLWithViewsQE {
	return MySQLWithViewsQE{
		ds: ds,
	}
}

// Query ...
func (qe MySQLWithViewsQE) Query(query string) (interface{}, error) {
	projection := []string{"title", "description", "short_id", "user_id", "vote_sum"}

	rows, err := qe.ds.Db.Query(query)
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(projection))
	scanArgs := make([]interface{}, len(projection))
	result := make([]map[string]string, 0)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]string)
		for i, col := range values {
			if col != nil {
				row[projection[i]] = string(col)
			}
		}
		result = append(result, row)
	}

	return result, nil
}

// Close ...
func (qe MySQLWithViewsQE) Close() {
	qe.ds.Db.Close()
}

// ------------------ MySQL (no MVs) -----------------

// MySQLPlainQE ...
type MySQLPlainQE struct {
	ds *datastore.Datastore
}

// NewMySQLPlainQE ...
func NewMySQLPlainQE(ds *datastore.Datastore) MySQLPlainQE {
	return MySQLPlainQE{
		ds: ds,
	}
}

// Query ...
func (qe MySQLPlainQE) Query(query string) (interface{}, error) {
	projection := []string{"story_id", "title", "description", "short_id", "vote_sum"}

	limit := -1
	queryStr := fmt.Sprintf("SELECT story_id, s.title, s.description, s.short_id, vote_sum "+
		"FROM stories s "+
		"JOIN ( "+
		"SELECT v.story_id, SUM(v.vote) as vote_sum "+
		"FROM votes v "+
		"WHERE v.comment_id IS NULL "+
		"GROUP BY v.story_id) "+
		"vc ON s.id = vc.story_id "+
		"ORDER BY vote_sum DESC "+
		"LIMIT %d",
		limit)

	rows, err := qe.ds.Db.Query(queryStr)
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(projection))
	scanArgs := make([]interface{}, len(projection))
	result := make([]map[string]string, 0)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]string)
		for i, col := range values {
			if col != nil {
				row[projection[i]] = string(col)
			}
		}
		result = append(result, row)
	}

	return result, nil
}

// Close ...
func (qe MySQLPlainQE) Close() {
	qe.ds.Db.Close()
}
