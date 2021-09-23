package newenemy

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/authzed/grpcutil"
	"github.com/authzed/spicedb/e2e"
	"google.golang.org/grpc"
)

type SpiceDb struct {
	id            string
	presharedKey  string
	datastore     string
	uri           string
	grpcPort      int
	internalPort  int
	metricsPort   int
	dashboardPort int
	pid           int
	cancel        context.CancelFunc
	client        Client
}

func (s *SpiceDb) Start(ctx context.Context, logprefix string, args ...string) error {
	logfile, err := e2e.File(ctx, fmt.Sprintf("spicedb-%s-%s.log", logprefix, s.id))
	if err != nil {
		return err
	}
	cmd := []string{
		"./spicedb",
		"serve",
		"--log-level=debug",
		"--grpc-preshared-key=" + s.presharedKey,
		"--grpc-no-tls",
		"--datastore-engine=" + s.datastore,
		"--datastore-conn-uri=" + s.uri,
		fmt.Sprintf("--grpc-addr=:%d", s.grpcPort),
		fmt.Sprintf("--internal-grpc-addr=:%d", s.internalPort),
		fmt.Sprintf("--metrics-addr=:%d", s.metricsPort),
		fmt.Sprintf("--dashboard-addr=:%d", s.dashboardPort),
	}

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.pid, err = e2e.GoRun(ctx, logfile, logfile, append(cmd, args...)...)
	return err
}

func (s *SpiceDb) Stop() error {
	if s.pid < 1 {
		return fmt.Errorf("can't stop an unstarted spicedb")
	}
	s.cancel()
	return nil
}

func (s *SpiceDb) Connect(ctx context.Context, out io.Writer) error {
	if s.pid < 1 {
		return fmt.Errorf("can't create client for unstarted spicedb")
	}

	addr := net.JoinHostPort("localhost", strconv.Itoa(s.grpcPort))
	e2e.WaitForServerReady(addr, out)

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithBlock(), grpc.WithInsecure(),
		grpcutil.WithInsecureBearerToken(s.presharedKey))
	if err != nil {
		return err
	}
	s.client = NewClient(conn)
	return nil
}

func (s *SpiceDb) Client() Client {
	return s.client
}

type SpiceCluster []*SpiceDb

func NewSpiceClusterFromCockroachCluster(c CockroachCluster, dbName, presharedKey string, ports []int) SpiceCluster {
	ss := make([]*SpiceDb, 0, len(c))
	if ports == nil {
		ports = []int{50051, 9090, 8090}
	}
	for i := 0; i < len(c); i++ {
		ss = append(ss, &SpiceDb{
			id:            strconv.Itoa(i + 1),
			presharedKey:  presharedKey,
			datastore:     "cockroachdb",
			uri:           c[i].ConnectionString(dbName),
			grpcPort:      ports[0] + 2*i,
			internalPort:  ports[0] + 2*i + 1,
			metricsPort:   ports[1] + i,
			dashboardPort: ports[2] + i,
		})
	}
	return ss
}

func (c *SpiceCluster) Start(ctx context.Context, out io.Writer, prefix string, args ...string) error {
	for _, s := range *c {
		fmt.Fprintln(out, "starting spice node", s.id)
		if err := s.Start(ctx, prefix, args...); err != nil {
			return err
		}
	}
	return nil
}

func (c *SpiceCluster) Stop(out io.Writer) error {
	for _, s := range *c {
		fmt.Fprintln(out, "stopping spice node", s.id)
		if err := s.Stop(); err != nil {
			return err
		}
	}
	return nil
}

func (c *SpiceCluster) Connect(ctx context.Context, out io.Writer) error {
	for _, s := range *c {
		fmt.Fprintln(out, "connecting to", s.grpcPort)
		if err := s.Connect(ctx, out); err != nil {
			return err
		}
	}
	return nil
}

func MigrateHead(ctx context.Context, datastore, uri string) error {
	for i := 0; i < 5; i++ {
		if err := e2e.Run(ctx, os.Stdout, os.Stderr,
			"./spicedb",
			"migrate", "head", "--datastore-engine="+datastore,
			"--datastore-conn-uri="+uri,
		); err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("failed to migrate spicedb")
}
