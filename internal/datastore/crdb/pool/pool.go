package pool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ccoveille/go-safecast"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"

	"github.com/authzed/spicedb/internal/datastore/postgres/common"
	log "github.com/authzed/spicedb/internal/logging"
)

var resetHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "crdb_client_resets",
	Help:    "cockroachdb client-side tx reset distribution",
	Buckets: []float64{0, 1, 2, 5, 10, 20, 50},
})

func init() {
	prometheus.MustRegister(resetHistogram)
}

type ctxDisableRetries struct{}

var CtxDisableRetries ctxDisableRetries

type RetryPool struct {
	pool          *pgxpool.Pool
	id            string
	healthTracker *NodeHealthTracker

	sync.RWMutex
	maxRetries  uint8
	nodeForConn map[*pgx.Conn]uint32
	gc          map[*pgx.Conn]struct{}
}

func NewRetryPool(ctx context.Context, name string, config *pgxpool.Config, healthTracker *NodeHealthTracker, maxRetries uint8, connectRate time.Duration) (*RetryPool, error) {
	config = config.Copy()
	p := &RetryPool{
		id:            name,
		maxRetries:    maxRetries,
		healthTracker: healthTracker,
		nodeForConn:   make(map[*pgx.Conn]uint32, 0),
		gc:            make(map[*pgx.Conn]struct{}, 0),
	}

	limiter := rate.NewLimiter(rate.Every(connectRate), 1)
	afterConnect := config.AfterConnect
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if afterConnect != nil {
			if err := afterConnect(ctx, conn); err != nil {
				return err
			}
		}

		p.Lock()
		defer p.Unlock()

		delete(p.nodeForConn, conn)
		delete(p.gc, conn)

		healthTracker.SetNodeHealth(nodeID(conn), true)

		if err := limiter.Wait(ctx); err != nil {
			return err
		}

		p.nodeForConn[conn] = nodeID(conn)

		return nil
	}

	// if we attempt to acquire or release a connection that has been marked for
	// GC, return false to tell the underlying pool to destroy the connection
	gcConnection := func(conn *pgx.Conn) bool {
		p.RLock()
		_, ok := p.gc[conn]
		p.RUnlock()
		if ok {
			p.Lock()
			delete(p.gc, conn)
			delete(p.nodeForConn, conn)
			p.Unlock()
		}
		return !ok
	}

	beforeAcquire := config.BeforeAcquire
	config.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
		if beforeAcquire != nil {
			if !beforeAcquire(ctx, conn) {
				return false
			}
		}

		return gcConnection(conn)
	}

	afterRelease := config.AfterRelease
	config.AfterRelease = func(conn *pgx.Conn) bool {
		if afterRelease != nil {
			if !afterRelease(conn) {
				return false
			}
		}
		return gcConnection(conn)
	}

	beforeClose := config.BeforeClose
	config.BeforeClose = func(conn *pgx.Conn) {
		if beforeClose != nil {
			beforeClose(conn)
		}
		p.Lock()
		defer p.Unlock()
		delete(p.nodeForConn, conn)
		delete(p.gc, conn)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	p.pool = pool
	return p, nil
}

// ID returns a string identifier for this pool for use in metrics and logs.
func (p *RetryPool) ID() string {
	return p.id
}

// MaxConns returns the MaxConns configured on the underlying pool
func (p *RetryPool) MaxConns() uint32 {
	// This should be non-negative
	maxConns, _ := safecast.ToUint32(p.pool.Config().MaxConns)
	return maxConns
}

// MinConns returns the MinConns configured on the underlying pool
func (p *RetryPool) MinConns() uint32 {
	// This should be non-negative
	minConns, _ := safecast.ToUint32(p.pool.Config().MinConns)
	return minConns
}

// ExecFunc is a replacement for pgxpool.Pool.Exec that allows resetting the
// connection on error, or retrying on a retryable error.
func (p *RetryPool) ExecFunc(ctx context.Context, tagFunc func(ctx context.Context, tag pgconn.CommandTag, err error) error, sql string, arguments ...any) error {
	return p.withRetries(ctx, func(conn *pgxpool.Conn) error {
		tag, err := conn.Conn().Exec(ctx, sql, arguments...)
		return tagFunc(ctx, tag, err)
	})
}

// QueryFunc is a replacement for pgxpool.Pool.Query that allows resetting the
// connection on error, or retrying on a retryable error.
func (p *RetryPool) QueryFunc(ctx context.Context, rowsFunc func(ctx context.Context, rows pgx.Rows) error, sql string, optionsAndArgs ...any) error {
	return p.withRetries(ctx, func(conn *pgxpool.Conn) error {
		rows, err := conn.Conn().Query(ctx, sql, optionsAndArgs...)
		if err != nil {
			return err
		}
		defer rows.Close()
		return rowsFunc(ctx, rows)
	})
}

// QueryRowFunc is a replacement for pgxpool.Pool.QueryRow that allows resetting
// the connection on error, or retrying on a retryable error.
func (p *RetryPool) QueryRowFunc(ctx context.Context, rowFunc func(ctx context.Context, row pgx.Row) error, sql string, optionsAndArgs ...any) error {
	return p.withRetries(ctx, func(conn *pgxpool.Conn) error {
		return rowFunc(ctx, conn.Conn().QueryRow(ctx, sql, optionsAndArgs...))
	})
}

// BeginFunc is a replacement for pgxpool.BeginFunc that allows resetting
// the connection on error, or retrying on a retryable error.
func (p *RetryPool) BeginFunc(ctx context.Context, txFunc func(pgx.Tx) error) error {
	return p.BeginTxFunc(ctx, pgx.TxOptions{}, txFunc)
}

// BeginTxFunc is a replacement for  pgxpool.BeginTxFunc that allows resetting
// the connection on error, or retrying on a retryable error.
func (p *RetryPool) BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, txFunc func(pgx.Tx) error) error {
	return p.withRetries(ctx, func(conn *pgxpool.Conn) error {
		tx, err := conn.BeginTx(ctx, txOptions)
		if err != nil {
			return err
		}

		return beginFuncExec(ctx, tx, txFunc)
	})
}

// AcquireAllIdle returns all idle connections from the underlying pgxpool.Pool
func (p *RetryPool) AcquireAllIdle(ctx context.Context) []*pgxpool.Conn {
	return p.pool.AcquireAllIdle(ctx)
}

// Config returns a copy of the underlying pgxpool.Pool config
func (p *RetryPool) Config() *pgxpool.Config {
	return p.pool.Config()
}

// Close closes the underlying pgxpool.Pool
func (p *RetryPool) Close() {
	p.pool.Close()
}

// Stat returns the underlying pgxpool.Pool stats
func (p *RetryPool) Stat() *pgxpool.Stat {
	return p.pool.Stat()
}

// Node returns the id for a connection
func (p *RetryPool) Node(conn *pgx.Conn) uint32 {
	p.RLock()
	defer p.RUnlock()
	id, ok := p.nodeForConn[conn]
	if !ok {
		return 0
	}
	return id
}

// Range applies a function to every entry in the connection list
func (p *RetryPool) Range(f func(conn *pgx.Conn, nodeID uint32)) {
	p.RLock()
	defer p.RUnlock()
	for k, v := range p.nodeForConn {
		f(k, v)
	}
}

// withRetries acquires a connection and attempts the request multiple times
func (p *RetryPool) withRetries(ctx context.Context, fn func(conn *pgxpool.Conn) error) error {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		if conn != nil {
			conn.Release()
		}
		return fmt.Errorf("error acquiring connection from pool: %w", err)
	}
	defer func() {
		if conn != nil {
			conn.Release()
		}
	}()

	var retries uint8
	defer func() {
		resetHistogram.Observe(float64(retries))
	}()

	maxRetries := p.maxRetries
	if disabled, ok := ctx.Value(CtxDisableRetries).(bool); ok && disabled {
		maxRetries = 0
	}

	for retries = uint8(0); retries <= maxRetries; retries++ {
		err = wrapRetryableError(ctx, fn(conn))
		if err == nil {
			conn.Release()
			if retries > 0 {
				log.Ctx(ctx).Info().Uint8("retries", retries).Msg("resettable database error succeeded after retry")
			}
			return nil
		}

		var (
			resettable *ResettableError
			retryable  *RetryableError
		)
		if errors.As(err, &resettable) || conn.Conn().IsClosed() {
			log.Ctx(ctx).Info().Err(err).Uint8("retries", retries).Msg("resettable error")

			nodeID := p.Node(conn.Conn())
			p.GC(conn.Conn())
			conn.Release()

			// After a resettable error, mark the node as unhealthy
			// The health tracker enforces an error rate, so a single request
			// failing will not mark the node as globally unhealthy.
			if nodeID > 0 {
				p.healthTracker.SetNodeHealth(nodeID, false)
			}

			common.SleepOnErr(ctx, err, retries)

			conn, err = p.acquireFromDifferentNode(ctx, nodeID)
			if err != nil {
				return fmt.Errorf("error acquiring connection from pool after retry %d: %w", retries, err)
			}
			continue
		}
		if errors.As(err, &retryable) {
			log.Ctx(ctx).Info().Err(err).Uint8("retries", retries).Msg("retryable error")
			common.SleepOnErr(ctx, err, retries)
			continue
		}
		conn.Release()
		// error is not resettable or retryable
		log.Ctx(ctx).Warn().Err(err).Uint8("retries", retries).Msg("error is not resettable or retryable")
		return err
	}
	return &MaxRetryError{MaxRetries: maxRetries, LastErr: err}
}

// GC marks a connection for destruction on the next Acquire.
// BeforeAcquire can signal to the pool to close the connection and clean up
// the reference in the pool at the same time, so we lazily GC connections
// instead of closing the connection directly.
func (p *RetryPool) GC(conn *pgx.Conn) {
	p.Lock()
	defer p.Unlock()
	p.gc[conn] = struct{}{}
	delete(p.nodeForConn, conn)
}

func (p *RetryPool) acquireFromDifferentNode(ctx context.Context, nodeID uint32) (*pgxpool.Conn, error) {
	log.Ctx(ctx).Trace().Uint32("node_id", nodeID).Msg("acquiring a connection from a different node")
	for {
		conn, err := p.pool.Acquire(ctx)
		if err != nil {
			if conn != nil {
				conn.Release()
			}
			return nil, err
		}
		if p.healthTracker.HealthyNodeCount() <= 1 {
			log.Ctx(ctx).Info().Msg("less than 2 healthy nodes, not attempting to get a connection from a different node")
			return conn, nil
		}
		id := p.Node(conn.Conn())
		if id == nodeID {
			conn.Release()
			continue
		}
		log.Ctx(ctx).Trace().Uint32("old node id", nodeID).Uint32("new node id", id).Msg("acquired a connection from a different node")
		return conn, nil
	}
}

// IsRetryableError returns whether the given CRDB error is a retryable error.
func IsRetryableError(ctx context.Context, err error) bool {
	sqlState := sqlErrorCode(err)
	if sqlState == "" {
		log.Ctx(ctx).Debug().Err(err).Msg("couldn't determine a sqlstate error code")
	}

	// Retryable errors: the transaction should be retried but no new connection
	// is needed.
	if sqlState == CrdbRetryErrCode ||
		// Error encountered when crdb nodes have large clock skew
		(sqlState == CrdbUnknownSQLState && strings.Contains(err.Error(), CrdbClockSkewMessage)) {
		return true
	}

	return false
}

// IsResettableError returns whether the given CRDB error is a resettable error.
func IsResettableError(ctx context.Context, err error) bool {
	// detect when an error is likely due to a node taken out of service
	if strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "unexpected EOF") ||
		strings.Contains(err.Error(), "conn closed") ||
		strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "connection reset by peer") {
		return true
	}

	sqlState := sqlErrorCode(err)
	if sqlState == "" {
		log.Ctx(ctx).Debug().Err(err).Msg("couldn't determine a sqlstate error code")
	}

	// Ambiguous result error includes connection closed errors
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#result-is-ambiguous
	if sqlState == CrdbAmbiguousErrorCode ||
		// Reset on node draining
		sqlState == CrdbServerNotAcceptingClients {
		return true
	}

	return false
}

func wrapRetryableError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	// if context is errored or timed out, return immediately
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) {
		return err
	}

	if IsResettableError(ctx, err) {
		return &ResettableError{Err: err}
	}

	if IsRetryableError(ctx, err) {
		return &RetryableError{Err: err}
	}
	return err
}
