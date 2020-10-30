package datastore

import (
	"database/sql"
	"fmt"
	"net"
	"time"

	//
	_ "github.com/go-sql-driver/mysql"
)

// Datastore ...
type Datastore struct {
	Db *sql.DB
}

// NewDatastore ...
func NewDatastore(endpoint, datastoreDB, accessKeyID, secretAccessKey string) (Datastore, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		accessKeyID,
		secretAccessKey,
		endpoint,
		datastoreDB,
	)

	for {
		c, err := net.DialTimeout("tcp", endpoint, time.Second)
		if err != nil {
			time.Sleep(1 * time.Second)
			fmt.Println("retying connecting to ", endpoint)
		} else {
			c.Close()
			break
		}
	}

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return Datastore{}, err
	}

	db.SetMaxIdleConns(1024)
	db.SetMaxOpenConns(1024)
	db.SetConnMaxLifetime(10 * time.Minute)

	return Datastore{Db: db}, nil
}

// StoryVote ...
func (ds Datastore) StoryVote(userID int, storyID int64, vote int) error {
	query := fmt.Sprintf("INSERT INTO votes (story_id, vote, user_id) VALUES (%d, %d, %d)", storyID, vote, userID)
	var err error
	_, err = ds.Db.Exec(query)
	return err
}

// Adduser ...
func (ds Datastore) Adduser(username string) error {
	query := fmt.Sprintf("INSERT INTO users (username) VALUES ('%s')", username)
	var err error
	_, err = ds.Db.Exec(query)
	return err
}

// Submit ...
func (ds Datastore) Submit(userID int, title, description, shortID string) error {
	query := fmt.Sprintf("INSERT INTO stories (user_id, title, description, short_id) VALUES (%d, '%s', '%s', '%s')", userID, title, description, shortID)
	var err error
	_, err = ds.Db.Exec(query)
	return err
}

// Comment ...
func (ds Datastore) Comment(userID int, storyID int64, comment string) error {
	query := fmt.Sprintf("INSERT INTO comments (user_id, story_id, comment) VALUES (%d, %d, '%s')", userID, storyID, comment)
	fmt.Println(query)
	var err error
	_, err = ds.Db.Exec(query)
	return err
}

// Get ...
func (ds Datastore) Get(table, projection string, predicate map[string]interface{}) (interface{}, error) {

	whereStmt := ""
	whereValues := make([]interface{}, len(predicate))
	i := 0

	for attrKey, val := range predicate {
		whereStmt += fmt.Sprintf("%s = ? ", attrKey)
		if len(predicate) > 1 && i < len(predicate)-1 {
			whereStmt += "AND "
		}
		whereValues[i] = val
		i++
	}

	query := "SELECT " + projection + " FROM " + table + " WHERE " + whereStmt
	stmtSelect, err := ds.Db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmtSelect.Close()

	var destValue interface{}
	err = stmtSelect.QueryRow(whereValues...).Scan(&destValue)

	return destValue, err
}
