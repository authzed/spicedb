package cockroach

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/e2e"
)

//go:generate go run github.com/ecordell/optgen -output cockroach_options.go . Cockroach

// Node represents a single cockroachdb instance
type Node struct {
	Peers     []string
	Addr      string
	Httpaddr  string
	ID        string
	MaxOffset time.Duration
	Cancel    context.CancelFunc
	// only available after Start()
	pid  int
	conn *pgx.Conn
}

// Start starts the cockroach instance with exec
func (c *Node) Start(ctx context.Context) error {
	logfile, err := e2e.File(ctx, fmt.Sprintf("crdb-%s.log", c.ID))
	if err != nil {
		return err
	}
	cmd := []string{
		"./cockroach",
		"start",
		"--store=type=mem,size=640MiB",
		"--logtostderr",
		"--insecure",
		"--listen-addr=" + c.Addr,
		"--http-addr=" + c.Httpaddr,
		"--join=" + strings.Join(c.Peers, ","),
		"--max-offset=" + c.MaxOffset.String(),
	}

	ctx, cancel := context.WithCancel(ctx)
	c.Cancel = cancel
	c.pid, err = e2e.GoRun(ctx, logfile, logfile, cmd...)
	return err
}

func (c *Node) Stop() error {
	if c.pid < 1 {
		return fmt.Errorf("can't stop an unstarted crdb")
	}
	c.Cancel()
	return nil
}

// ConnectionString returns the postgres db URI for this cluster
func (c *Node) ConnectionString(dbName string) string {
	return fmt.Sprintf("postgresql://root@%s/%s?sslmode=disable", c.Addr, dbName)
}

// Connect connects directly to the cockroach instance and caches the connection
func (c *Node) Connect(ctx context.Context, _ io.Writer, dbName string) error {
	if c.pid < 1 {
		return fmt.Errorf("can't connect to unstarted cockroach")
	}

	conn, err := pgx.Connect(ctx, c.ConnectionString(dbName))
	if err != nil {
		return err
	}

	c.conn = conn
	return nil
}

// Conn returns the current connection. Must only be called after Connect().
func (c *Node) Conn() *pgx.Conn {
	return c.conn
}

// NodeID returns the cockroach-internal node id for this connection. This is
// the value that is referenced by other crdb metadata to identify range leader,
// follower nodes, etc.
func (c *Node) NodeID(ctx context.Context) (int, error) {
	var nodeID string
	if err := c.conn.QueryRow(ctx, "SHOW node_id").Scan(&nodeID); err != nil {
		return -1, err
	}

	i, err := strconv.Atoi(nodeID)
	if err != nil {
		return -1, err
	}
	return i, nil
}

// Cluster represents a set of Node nodes configured to talk to
// each other.
type Cluster []*Node

// NewCluster returns a pre-configured cluster of the given size.
func NewCluster(n int) Cluster {
	cs := make([]*Node, 0, n)
	peers := make([]string, 0, n)

	port := 26257
	http := 8080
	for i := 0; i < n; i++ {
		addr := net.JoinHostPort("localhost", strconv.Itoa(port+i))
		peers = append(peers, addr)
		cs = append(cs, &Node{
			ID:        strconv.Itoa(i + 1),
			Addr:      addr,
			Httpaddr:  net.JoinHostPort("localhost", strconv.Itoa(http+i)),
			MaxOffset: 5 * time.Second,
		})
	}
	for i := range cs {
		cs[i].Peers = peers
	}
	return cs
}

// Started returns true if all instances have been started
func (cs Cluster) Started() bool {
	for _, c := range cs {
		if c.pid <= 0 {
			return false
		}
	}
	return true
}

// Stop stops the entire cluster of spicedb instances
func (cs Cluster) Stop(out io.Writer) error {
	for i, c := range cs {
		fmt.Fprintln(out, "stopping crdb node", i)
		if err := c.Stop(); err != nil {
			return err
		}
	}
	return nil
}

// Init runs the cockroach init command against the cluster
func (cs Cluster) Init(ctx context.Context, out, errOut io.Writer) {
	// this retries until it succeeds, it won't return unless it does
	if err := e2e.Run(ctx, out, errOut, "./cockroach",
		"init",
		"--insecure",
		"--host="+cs[0].Addr,
	); err != nil {
		panic(err)
	}
}

// SQL runs the set of SQL commands against the cluster
func (cs Cluster) SQL(ctx context.Context, out, errOut io.Writer, sql ...string) error {
	for _, s := range sql {
		if err := e2e.Run(ctx, out, errOut,
			"./cockroach", "sql", "--insecure", "--host="+cs[0].Addr,
			"-e", s,
		); err != nil {
			return err
		}
	}
	return nil
}

// NetworkDelay simulates network delay against the selected node
func (cs Cluster) NetworkDelay(ctx context.Context, out io.Writer, node int, duration time.Duration) error {
	_, port, err := net.SplitHostPort(cs[node].Addr)
	if err != nil {
		return err
	}
	return e2e.Run(ctx, out, out,
		"sudo",
		"./chaosd",
		"attack",
		"network",
		"delay",
		"-l="+duration.String(),
		"-e="+port,
		"-d=lo",
		"-p=tcp",
	)
}

// TimeDelay adds a skew to the clock of the given node
func (cs Cluster) TimeDelay(ctx context.Context, out io.Writer, node int, duration time.Duration) error {
	return e2e.Run(ctx, out, out,
		"sudo",
		"./chaosd",
		"attack",
		"clock",
		fmt.Sprintf("--pid=%d", cs[node].pid),
		fmt.Sprintf("--time-offset=%s", duration),
		"--clock-ids-slice=CLOCK_REALTIME,CLOCK_MONOTONIC",
	)
}
