package mysql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"time"

	log "github.com/authzed/spicedb/internal/logging"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	connectHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "mysql_connect_duration",
		Help:      "distribution in seconds of time spent opening a new MySQL connection.",
		Buckets:   []float64{0.01, 0.1, 0.5, 1, 5, 10, 25, 60, 120},
	})
	connectCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "mysql_connect_count_total",
		Help:      "number of mysql connections opened.",
	}, []string{"success"})
)

// instrumentedConnector wraps the default MySQL driver connector
// to get metrics and tracing when creating a new connection
type instrumentedConnector struct {
	conn driver.Connector
	drv  driver.Driver
}

func (d *instrumentedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	ctx, span := tracer.Start(ctx, "openMySQLConnection")
	defer span.End()

	startTime := time.Now()
	defer func() {
		connectHistogram.Observe(time.Since(startTime).Seconds())
	}()

	conn, err := d.conn.Connect(ctx)
	connectCount.WithLabelValues(strconv.FormatBool(err == nil)).Inc()
	if err != nil {
		span.RecordError(err)
		log.Ctx(ctx).Error().Err(err).Msg("failed to open mysql connection")
		return nil, fmt.Errorf("failed to open connection to mysql: %w", err)
	}

	return conn, nil
}

func (d *instrumentedConnector) Driver() driver.Driver {
	return d.drv
}

func instrumentConnector(c driver.Connector) (driver.Connector, error) {
	err := prometheus.Register(connectHistogram)
	if err != nil {
		return nil, fmt.Errorf("unable to register metric: %w", err)
	}

	err = prometheus.Register(connectCount)
	if err != nil {
		return nil, fmt.Errorf("unable to register metric: %w", err)
	}

	return &instrumentedConnector{
		conn: c,
		drv:  c.Driver(),
	}, nil
}

type sessionVariableConnector struct {
	conn driver.Connector
	drv  driver.Driver

	statements []string
}

func (s *sessionVariableConnector) Connect(ctx context.Context) (driver.Conn, error) {
	ctx, span := tracer.Start(ctx, "setSessionVariables")
	defer span.End()

	conn, err := s.conn.Connect(ctx)
	if err != nil {
		span.RecordError(err)
		log.Ctx(ctx).Error().Err(err).Msg("failed to open db connection")
		return nil, fmt.Errorf("failed to open connection to db: %w", err)
	}

	// The go mysql driver implements the ExecerContext interface, assert that here.
	execConn := conn.(driver.ExecerContext)

	for _, stmt := range s.statements {
		if _, err := execConn.ExecContext(ctx, stmt, nil); err != nil {
			return nil, fmt.Errorf("unable to execute statement `%s`: %w", stmt, err)
		}
	}

	return conn, nil
}

func (s *sessionVariableConnector) Driver() driver.Driver {
	return s.drv
}

func addSessionVariables(c driver.Connector, variables map[string]string) (driver.Connector, error) {
	statements := make([]string, 0, len(variables))
	for sessionVar, value := range variables {
		statements = append(statements, "SET SESSION "+sessionVar+"="+value)
	}

	return &sessionVariableConnector{
		conn:       c,
		drv:        c.Driver(),
		statements: statements,
	}, nil
}
