package newenemy

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

type Cockroach struct {
	peers    []string
	addr     string
	httpaddr string
	id       string
	// only available after Start()
	pid  int
	conn *pgx.Conn
}

func (c *Cockroach) Start(ctx context.Context) error {
	logfile, err := e2e.File(ctx, fmt.Sprintf("crdb-%s.log", c.id))
	if err != nil {
		return err
	}
	cmd := []string{
		"./cockroach",
		"start",
		"--store=node" + c.id,
		"--logtostderr",
		"--insecure",
		"--listen-addr=" + c.addr,
		"--http-addr=" + c.httpaddr,
		"--join=" + strings.Join(c.peers, ","),
	}

	c.pid, err = e2e.GoRun(ctx, logfile, logfile, cmd...)
	return err
}

func (c *Cockroach) ConnectionString(dbName string) string {
	return fmt.Sprintf("postgresql://root@%s/%s?sslmode=disable", c.addr, dbName)
}

func (c *Cockroach) Connect(ctx context.Context, out io.Writer, dbName string) error {
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

func (s *Cockroach) Conn() *pgx.Conn {
	return s.conn
}

type CockroachCluster []*Cockroach

func NewCockroachCluster(n int) CockroachCluster {
	cs := make([]*Cockroach, 0, n)
	peers := make([]string, 0, n)
	port := 26257
	http := 8080
	for i := 0; i < n; i++ {
		addr := net.JoinHostPort("localhost", strconv.Itoa(port+i))
		peers = append(peers, addr)
		cs = append(cs, &Cockroach{
			id:       strconv.Itoa(i + 1),
			addr:     addr,
			httpaddr: net.JoinHostPort("localhost", strconv.Itoa(http+i)),
		})
	}
	for i := range cs {
		cs[i].peers = peers
	}
	return cs
}

func (cs CockroachCluster) Started() bool {
	for _, c := range cs {
		if c.pid <= 0 {
			return false
		}
	}
	return true
}

func (cs CockroachCluster) Init(ctx context.Context, out, errOut io.Writer) {
	// this retries until it succeeds, it won't return unless it does
	e2e.Run(ctx, out, errOut, "./cockroach",
		"init",
		"--insecure",
		"--host="+cs[0].addr,
	)
}

func (cs CockroachCluster) Sql(ctx context.Context, out, errOut io.Writer, sql ...string) error {
	for _, s := range sql {
		if err := e2e.Run(ctx, out, errOut,
			"./cockroach", "sql", "--insecure", "--host="+cs[0].addr,
			"-e", s,
		); err != nil {
			return err
		}
	}
	return nil
}

func (cs CockroachCluster) NetworkDelay(ctx context.Context, out io.Writer, node int, duration time.Duration) error {
	_, port, err := net.SplitHostPort(cs[node].addr)
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

func (cs CockroachCluster) TimeDelay(ctx context.Context, out io.Writer, node int, duration time.Duration) error {
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
