// Package pgbalancer is a balancer of the postgres databases with RPS limiter.
// > Requires golang version >= 1.9.
// > Warning: This balancer is the client-side only, and it assumed that all nodes have been replicated by database-side.
//
// This package implements only four methods to select information from a cluster (Query, QueryContext, QueryRow, QueryRowContext).
// Also pgbalancer support few methods of `sql.DB` (Close, SetMaxOpenConns, SetMaxIdleConns and so on).
//
// Requires golang version >= 1.9.

package pgbalancer

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type Cluster struct {
	l           *log.Logger
	nodes       sync.Map
	idleNode    chan *Node
	mu          sync.RWMutex
	limitRPS    int
	maxIdleConn int
	maxOpenConn int
	nodeLen     int
}

var (
	ErrDuplicateNodeName = fmt.Errorf("node name is already exist")
)

// NewCluster initializes a new cluster of databases.
// The parameter limitRPS sets a limit of RPS to every databases.
// If limitRPS equals to 0 then function sets no limits to requests.
// Also function sets default output as os.Stdout. See: `SetLogger(w io.Writer)`
// to change a logger to your own.
func NewCluster(limitRPS int) *Cluster {
	defaultLogger := &log.Logger{}
	defaultLogger.SetOutput(os.Stdout)
	c := &Cluster{
		//nodes:    make(map[string]*Node, 0),
		nodes:    sync.Map{},
		l:        defaultLogger,
		limitRPS: limitRPS,
		idleNode: make(chan *Node, 1),
	}
	go nodeSelector(c)
	return c
}

// SetLimitRPS sets RPS limits to every database in the cluster.
// If limitRPS equals to 0 then function sets no limits to requests.
func (c *Cluster) SetLimitRPS(limitRPS int) {
	c.mu.Lock()
	c.limitRPS = limitRPS
	c.mu.Unlock()
	c.nodes.Range(func(key, value interface{}) bool {
		value.(*Node).setLimit(limitRPS)
		return true
	})
}

// SetLogger sets the output of logger.
func (c *Cluster) SetLogger(w io.Writer) {
	c.l.SetOutput(w)
}

// Len returns the amounts of cluster's databases.
func (c *Cluster) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodeLen
}

// AddNode adds a node into the cluster with it's name
// and DSN string to a database.
// It may return an error, if the name of Node is already
// uses in the cluster.
// You may add a new node into the cluster at runtime.
func (c *Cluster) AddNode(name string, dsn string) error {
	if c.nodeExists(name) {
		return fmt.Errorf("%w: %s", ErrDuplicateNodeName, name)
	}
	node, err := newNode(name, dsn, c.limitRPS, c.maxIdleConn, c.maxOpenConn)
	if err != nil {
		return fmt.Errorf("failed to add a node: %w", err)
	}
	c.mu.Lock()
	c.nodeLen++
	c.nodes.Store(name, node)
	c.mu.Unlock()
	return nil
}

// RemoveNode removes a node from the cluster by it's name.
// You may remove nodes at runtime.
func (c *Cluster) RemoveNode(name string) {
	if c.nodeExists(name) {
		c.mu.Lock()
		c.nodeLen--
		c.nodes.Delete(name)
		c.mu.Unlock()
	}
}

func (c *Cluster) nodeExists(name string) bool {
	c.mu.RLock()
	_, found := c.nodes.Load(name)
	c.mu.RUnlock()
	return found
}

func nodeSelector(c *Cluster) {
	for {
		c.nodes.Range(func(key, value interface{}) bool {
			node, ok := value.(*Node)
			if !ok {
				c.l.Println("type assertion error: map stores different then *Node type")
				return false
			}
			if node.limiter.Allow() {
				c.idleNode <- node
			}
			return true
		})
	}
}

// Node fetches idle node for a sql query.
func (c *Cluster) Node() *Node {
	return <-c.idleNode
}

// Close closes all connections to the databases in the cluster.
// It returns all occurred errors.
func (c *Cluster) Close() error {
	var returnError error
	c.nodes.Range(func(key, value interface{}) bool {
		node, ok := value.(*Node)
		if !ok {
			c.l.Print("type assertion error: map stores different then *Node type")
			return true
		}
		if err := node.db.Close(); err != nil {
			returnError = fmt.Errorf("node %s: %w", key, err)
		}
		return true
	})
	return returnError
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool to all databases in the cluster.
// All new node will be set the value.
//
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns,
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit.
//
// If n <= 0, no idle connections are retained.
func (c *Cluster) SetMaxIdleConns(n int) {
	c.mu.Lock()
	c.maxIdleConn = n
	c.mu.Unlock()
	c.nodes.Range(func(key, value interface{}) bool {
		node, ok := value.(*Node)
		if !ok {
			c.l.Println("type assertion error: map stores different then *Node type")
			return true
		}
		node.mu.Lock()
		node.db.SetMaxIdleConns(n)
		node.mu.Unlock()
		return true
	})
}

// SetMaxOpenConns sets the maximum number of open connections to the database's cluster.
// All new node will be set the value.
//
// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than
// MaxIdleConns, then MaxIdleConns will be reduced to match the new
// MaxOpenConns limit.
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func (c *Cluster) SetMaxOpenConns(n int) {
	c.mu.Lock()
	c.maxOpenConn = n
	c.mu.Unlock()
	c.nodes.Range(func(key, value interface{}) bool {
		node, ok := value.(*Node)
		if !ok {
			c.l.Println("type assertion error: map stores different then *Node type")
			return true
		}
		node.mu.Lock()
		node.db.SetMaxOpenConns(n)
		node.mu.Unlock()
		return true
	})
}
