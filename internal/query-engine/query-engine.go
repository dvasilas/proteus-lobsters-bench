package queryengine

import (
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

// ProteusQE ...
type ProteusQE struct {
	proteusClient *proteusclient.Client
}

// MysqlQE ...
type MysqlQE struct {
	ds *datastore.Datastore
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

// Close ...
func (qe ProteusQE) Close() {
	qe.proteusClient.Close()
}

// ------------------ MySQL query engine ---------------

// NewMysqlQE ...
func NewMysqlQE(ds *datastore.Datastore) MysqlQE {
	return MysqlQE{
		ds: ds,
	}
}

// Query ...
func (qe MysqlQE) Query(query string) (interface{}, error) {
	rows, err := qe.ds.Db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	result := make([]map[string]interface{}, 0)

	for rows.Next() {

		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)

		row := make(map[string]interface{})
		for i, col := range values {
			if col != nil {
				row[columns[i]] = col
			}
		}

		for i, col := range columns {
			val := values[i]

			b, ok := val.([]byte)
			var v interface{}
			if ok {
				v = string(b)
			} else {
				v = val
			}
			row[col] = v
		}

		result = append(result, row)
	}

	return result, nil
}

// Close ...
func (qe MysqlQE) Close() {
	qe.ds.Db.Close()
}
