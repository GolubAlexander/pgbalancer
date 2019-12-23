package pgbalancer

import (
	"context"
	"database/sql"
	"time"
)

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// Query uses a free node as the physical db.
func (c *Cluster) Query(query string, args ...interface{}) (*sql.Rows, error) {
	node := c.Node()
	startTime := time.Now()
	r, err := node.db.Query(query, args...)
	stopTime := time.Now()
	c.l.Printf("node: %s; q: %s; t: %s", node.name, query, stopTime.Sub(startTime))
	return r, err
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// Query uses a free node as the physical db.
func (c *Cluster) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	node := c.Node()
	startTime := time.Now()
	r, err := node.db.QueryContext(ctx, query, args...)
	stopTime := time.Now()
	c.l.Printf("node: %s; q: %s; t: %s", node.name, query, stopTime.Sub(startTime))
	return r, err
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// Query uses a free node as the physical db.
func (c *Cluster) QueryRow(query string, args ...interface{}) *sql.Row {
	node := c.Node()
	startTime := time.Now()
	r := node.db.QueryRow(query, args...)
	stopTime := time.Now()
	c.l.Printf("node: %s; q: %s; t: %s", node.name, query, stopTime.Sub(startTime))
	return r
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// Query uses a free node as the physical db.
func (c *Cluster) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	node := c.Node()
	startTime := time.Now()
	r := node.db.QueryRowContext(ctx, query, args...)
	stopTime := time.Now()
	c.l.Printf("node: %s; q: %s; t: %s", node.name, query, stopTime.Sub(startTime))
	return r
}
