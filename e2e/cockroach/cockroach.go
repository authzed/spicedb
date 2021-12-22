package cockroach

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"

	"github.com/authzed/spicedb/e2e"
)

//go:generate go run github.com/ecordell/optgen -output cockroach_options.go . Cockroach

// Node represents a single cockroachdb instance
type Node struct {
	Peers    []string
	Addr     string
	Httpaddr string
	ID       string
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
		"--store=node" + c.ID,
		"--logtostderr",
		"--insecure",
		"--listen-addr=" + c.Addr,
		"--http-addr=" + c.Httpaddr,
		"--join=" + strings.Join(c.Peers, ","),
	}

	c.pid, err = e2e.GoRun(ctx, logfile, logfile, cmd...)
	return err
}

// ConnectionString returns the postgres db URI for this cluster
func (c *Node) ConnectionString(dbName string) string {
	return fmt.Sprintf("postgresql://root@%s/%s?sslmode=disable", c.Addr, dbName)
}

// Connect connects directly to the cockroach instance and caches the connection
func (c *Node) Connect(ctx context.Context, out io.Writer, dbName string) error {
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
	rows, err := c.conn.Query(ctx, "SHOW node_id")
	defer rows.Close()
	if err != nil {
		return -1, err
	}
	// despite being an int, crdb returns node id as a string
	var nodeID string
	for rows.Next() {
		if err := rows.Scan(&nodeID); err != nil {
			return -1, err
		}
		break
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
			ID:       strconv.Itoa(i + 1),
			Addr:     addr,
			Httpaddr: net.JoinHostPort("localhost", strconv.Itoa(http+i)),
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

// Init runs the cockroach init command against the cluster
func (cs Cluster) Init(ctx context.Context, out, errOut io.Writer) {
	// this retries until it succeeds, it won't return unless it does
	e2e.Run(ctx, out, errOut, "./cockroach",
		"init",
		"--insecure",
		"--host="+cs[0].Addr,
	)
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
	sec, nsec := secAndNSecFromDuration(duration)
	return e2e.Run(ctx, out, out,
		"sudo",
		"./watchmaker",
		fmt.Sprintf("--pid=%d", cs[node].pid),
		fmt.Sprintf("--sec_delta=%d", sec),
		fmt.Sprintf("--nsec_delta=%d", nsec),
		"--clk_ids=CLOCK_REALTIME,CLOCK_MONOTONIC",
	)
}

func secAndNSecFromDuration(duration time.Duration) (sec int64, nsec int64) {
	sec = duration.Nanoseconds() / 1e9
	nsec = duration.Nanoseconds() - (sec * 1e9)
	return
}
