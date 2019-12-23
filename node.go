package pgbalancer

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/lib/pq"
	"golang.org/x/time/rate"
)

type Node struct {
	name    string
	db      *sql.DB
	mu      sync.RWMutex
	limiter *rate.Limiter
}

func newNode(name string, dsn string, limitRPS int, maxIdleConn int, maxOpenConn int) (*Node, error) {

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	db.SetMaxIdleConns(maxIdleConn)
	db.SetMaxOpenConns(maxOpenConn)
	n := &Node{db: db, name: name}
	n.setLimit(limitRPS)
	return n, nil
}

func (n *Node) setLimit(limitRPS int) {
	l := rate.Limit(limitRPS)
	limiter := rate.NewLimiter(l, 1)
	n.limiter = limiter
}
