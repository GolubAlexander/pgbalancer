# pgbalancer
> *Requires golang version >= 1.9.*

> Warning: This balancer is the client-side only, and it assumed that all nodes have been replicated by database-side.

Package pgbalancer is a balancer of the postgres databases with RPS limiter.

This package implements only four methods to select information from a cluster (Query, QueryContext, QueryRow, QueryRowContext).
Also pgbalancer support few methods of `sql.DB` (Close, SetMaxOpenConns, SetMaxIdleConns and so on).

Pgbalancer contents a logger to write all queries. Log pattern is `node: %s; q: %s; t: %s` where `node` is node's name, 
`q` is query and `t` - time of running a query. You may use any logger which implements `log.Logger` from std lib. 

### How it works.

After initialization of the cluster it starts a goroutine with infinite loop. When a node is
available (by RPS limits) it puts this selected note to the channel with the buffer size 1. And that
infinite loop blocks till the selected node will be read by any query to the cluster.

### Examples.
```go
import "github.com/GolubAlexander/pgbalancer"
...
// Creates a new cluster with a limit 10 requests per seconds for a single node.
cl := pgbalancer.NewCluster(10)
```

```go
// You may use any favorite logger.
l := zerolog.New(os.Stderr)
l = l.With().Timestamp().Logger()
// and set logger to cluster
cl.SetLogger(l)
```
```go
// Adds nodes to the cluster. If name is already exists method will return ErrDuplicateNodeName.
dsn := "postgres://postgres:@localhost:5432/postgres?sslmode=disable"
if err := cl.AddNode("alpha", dsn); err != nil {
	panic(err)
}
```
```go
// Makes a query to the cluster (Query, QueryContext, QueryRow, QueryRowContext). 
// Returns `*sql.Rows` and error.
r, err := cl.Query("SELECT 1")
if err != nil {
	panic(err)
}
_ = r.Close()
```
```go
// Removes the node from the cluster by its name.
cl.RemoveNode("alpha")
```
```go
// Sets connection's options to all current nodes and to new ones. 
cl.SetMaxOpenConns(10)
cl.SetMaxIdleConns(10)
```
```go
// Closes all connections in the cluster.
if err := cl.Close(); err != nil {
	panic(err)
}
```