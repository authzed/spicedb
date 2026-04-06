package pool

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
	"weak"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"

	"github.com/authzed/spicedb/internal/datastore/postgres/common"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// pgxPool interface is the subset of pgxpool.Pool that RetryPool needs
type pgxPool interface {
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
	AcquireAllIdle(ctx context.Context) []*pgxpool.Conn
	Config() *pgxpool.Config
	Close()
	Stat() *pgxpool.Stat
}

// RetryPoolOption configures optional behavior on a RetryPool at construction time.
// It receives the pool config to modify and a pointer to the RetryPool being
// built so that options can set pool-level flags.
type RetryPoolOption func(*pgxpool.Config, *RetryPool)

// WithCancelHandler installs a cancelHandler on each connection in the pool.
// When the context is cancelled, the handler sends a PostgreSQL cancel request
// to CRDB so it stops executing the query early, without closing the connection.
// The connection is drained synchronously in withRetries before pool release.
//
// To avoid a data race between CancelRequest (which reads pid/secretKey) and
// pgconn's connectOne (which writes them), the handler only sends the cancel
// request after the connection is fully established. The pgconn-level
// AfterConnect hook (config.ConnConfig.AfterConnect) marks the handler ready
// immediately after connectOne completes; until then the net.Conn deadline set
// in HandleCancel is sufficient to unblock the caller.
func WithCancelHandler() RetryPoolOption {
	return func(config *pgxpool.Config, p *RetryPool) {
		p.cancelHandlerInstalled = true
		// handlers maps each connection's identity to its cancelHandler so that
		// the pgconn AfterConnect hook can mark it ready.
		//
		// Both key and value are weak pointers so the map does not prevent the
		// GC from collecting a failed connection's pgConn ↔ cancelHandler cycle.
		// On success, AfterConnect removes the entry via LoadAndDelete. On
		// failure (connectOne returns an error before AfterConnect fires), the
		// pgConn and cancelHandler become unreachable — the GC collects the
		// cycle, and AddCleanup then deletes the stale map entry.
		var handlers sync.Map // weak.Pointer[pgconn.PgConn] → weak.Pointer[cancelHandler]

		config.ConnConfig.BuildContextWatcherHandler = func(pgConn *pgconn.PgConn) ctxwatch.Handler {
			h := &cancelHandler{
				pgConn:        pgConn,
				deadlineDelay: 1 * time.Second,
			}
			key := weak.Make(pgConn)
			handlers.Store(key, weak.Make(h))
			// When h is collected (connection failed; the pgConn ↔ cancelHandler
			// cycle became unreachable), remove the stale map entry.
			runtime.AddCleanup(h, func(key weak.Pointer[pgconn.PgConn]) {
				handlers.Delete(key)
			}, key)
			return h
		}

		// Use the pgconn-level AfterConnect to mark the handler ready.
		// This fires inside pgconn.ConnectConfig, immediately after connectOne
		// writes pid/secretKey and before the connection is surfaced to pgxpool.
		// Using the pgconn hook avoids interfering with pgxpool.Config.AfterConnect
		// (which NewRetryPool wraps for pool housekeeping).
		prevPgconn := config.ConnConfig.AfterConnect
		config.ConnConfig.AfterConnect = func(ctx context.Context, pgConn *pgconn.PgConn) error {
			// connectOne has completed; pid/secretKey are written.
			// Mark the handler ready so HandleCancel may call CancelRequest.
			key := weak.Make(pgConn)
			if v, ok := handlers.LoadAndDelete(key); ok {
				if h := v.(weak.Pointer[cancelHandler]).Value(); h != nil {
					h.ready.Store(1)
				}
			}
			if prevPgconn != nil {
				return prevPgconn(ctx, pgConn)
			}
			return nil
		}
	}
}

var resetHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "crdb_client_resets",
	Help:    "Distribution of the number of client-side transaction restarts per transaction attempt. Restarts occur when CockroachDB returns a serialization failure (40001) and the driver retries the transaction from scratch. Sustained high values indicate transaction contention.",
	Buckets: []float64{0, 1, 2, 5, 10, 20, 50},
})

func init() {
	prometheus.MustRegister(resetHistogram)
}

type ctxDisableRetries struct{}

var (
	CtxDisableRetries ctxDisableRetries
	ErrAcquire        = errors.New("failed to acquire in time")
)

type RetryPool struct {
	pool          pgxPool
	id            string
	healthTracker *NodeHealthTracker

	// cancelHandlerInstalled is true when WithCancelHandler was passed to
	// NewRetryPool. Only when true does withRetries drain on cancellation;
	// without a cancel handler there is no in-flight CancelRequest to absorb.
	cancelHandlerInstalled bool

	// drainCancelledConnFn drains a cancelled connection before pool release.
	// Defaults to drainCancelledConn; overridable in tests.
	drainCancelledConnFn func(conn *pgxpool.Conn) bool

	sync.RWMutex
	maxRetries  uint8
	nodeForConn map[*pgx.Conn]uint32   // GUARDED_BY(RWMutex)
	gc          map[*pgx.Conn]struct{} // GUARDED_BY(RWMutex)
}

func NewRetryPool(ctx context.Context, name string, config *pgxpool.Config, healthTracker *NodeHealthTracker, maxRetries uint8, connectRate time.Duration, opts ...RetryPoolOption) (*RetryPool, error) {
	config = config.Copy()
	p := &RetryPool{
		id:                   name,
		maxRetries:           maxRetries,
		healthTracker:        healthTracker,
		nodeForConn:          make(map[*pgx.Conn]uint32, 0),
		gc:                   make(map[*pgx.Conn]struct{}, 0),
		drainCancelledConnFn: drainCancelledConn,
	}
	for _, opt := range opts {
		opt(config, p)
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

	beforeAcquire := config.BeforeAcquire                                   //nolint:staticcheck
	config.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool { //nolint:staticcheck
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
	return spiceerrors.MustSafecast[uint32](p.pool.Config().MaxConns)
}

// MinConns returns the MinConns configured on the underlying pool
func (p *RetryPool) MinConns() uint32 {
	// This should be non-negative
	return spiceerrors.MustSafecast[uint32](p.pool.Config().MinConns)
}

// ExecFunc is a replacement for pgxpool.pgxPool.Exec that allows resetting the
// connection on error, or retrying on a retryable error.
func (p *RetryPool) ExecFunc(ctx context.Context, tagFunc func(ctx context.Context, tag pgconn.CommandTag, err error) error, sql string, arguments ...any) error {
	return p.withRetries(ctx, 0, func(conn *pgxpool.Conn) error {
		return common.QuerierFuncsFor(conn.Conn()).ExecFunc(ctx, tagFunc, sql, arguments...)
	})
}

// QueryFunc is a replacement for pgxpool.pgxPool.Query that allows resetting the
// connection on error, or retrying on a retryable error.
func (p *RetryPool) QueryFunc(ctx context.Context, rowsFunc func(ctx context.Context, rows pgx.Rows) error, sql string, optionsAndArgs ...any) error {
	return p.withRetries(ctx, 0, func(conn *pgxpool.Conn) error {
		return common.QuerierFuncsFor(conn.Conn()).QueryFunc(ctx, rowsFunc, sql, optionsAndArgs...)
	})
}

// QueryRowFunc is a replacement for pgxpool.pgxPool.QueryRow that allows resetting
// the connection on error, or retrying on a retryable error.
func (p *RetryPool) QueryRowFunc(ctx context.Context, rowFunc func(ctx context.Context, row pgx.Row) error, sql string, optionsAndArgs ...any) error {
	return p.withRetries(ctx, 0, func(conn *pgxpool.Conn) error {
		return common.QuerierFuncsFor(conn.Conn()).QueryRowFunc(ctx, rowFunc, sql, optionsAndArgs...)
	})
}

// BeginFunc is a replacement for pgxpool.BeginFunc that allows resetting
// the connection on error, or retrying on a retryable error.
func (p *RetryPool) BeginFunc(ctx context.Context, txFunc func(pgx.Tx) error) error {
	return p.BeginTxFunc(ctx, pgx.TxOptions{}, txFunc)
}

// TryBeginFunc attempts to get a connection from the pool within acquisitionTimeout.
// If successful, it behaves like BeginFunc and will retry on errors.
// If unsuccessful, it returns ErrAcquire.
func (p *RetryPool) TryBeginFunc(ctx context.Context, acquireTimeout time.Duration, txFunc func(pgx.Tx) error) error {
	return p.withRetries(ctx, acquireTimeout, func(conn *pgxpool.Conn) error {
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return err
		}

		return beginFuncExec(ctx, tx, txFunc)
	})
}

// BeginTxFunc is a replacement for pgxpool.BeginTxFunc that allows resetting
// the connection on error, or retrying on a retryable error.
func (p *RetryPool) BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, txFunc func(pgx.Tx) error) error {
	return p.withRetries(ctx, 0, func(conn *pgxpool.Conn) error {
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
func (p *RetryPool) withRetries(ctx context.Context, acquireTimeout time.Duration, fn func(conn *pgxpool.Conn) error) error {
	acquireCtx := ctx
	acquireCancel := func() {}

	if acquireTimeout > 0 {
		acquireCtx, acquireCancel = context.WithTimeoutCause(context.Background(), acquireTimeout, ErrAcquire)
	}

	conn, err := p.pool.Acquire(acquireCtx)
	if err != nil {
		if conn != nil {
			conn.Release()
		}
		if acquireCtx.Err() != nil {
			err = context.Cause(acquireCtx)
		}
		acquireCancel()
		return fmt.Errorf("error acquiring connection from pool: %w", err)
	}
	acquireCancel()

	defer func() {
		if conn != nil {
			conn.Release()
		}
	}()

	// if top-level context was cancelled while we tried to acquire:
	if ctx.Err() != nil {
		return ctx.Err()
	}

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
			conn = nil // suppress the deferred release
			if retries > 0 {
				log.Ctx(ctx).Info().Uint8("retries", retries).Msg("resettable database error succeeded after retry")
			}
			return nil
		}

		var (
			resettable *ResettableError
			retryable  *RetryableError
		)
		if errors.As(err, &resettable) || (conn != nil && conn.Conn().IsClosed()) {
			log.Ctx(ctx).Info().Err(err).Uint8("retries", retries).Msg("resettable error")

			nodeID := p.Node(conn.Conn())
			p.GC(conn.Conn())
			conn.Release()
			conn = nil // will be reassigned by acquireFromDifferentNode below

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
		// Drain the connection before release when the context was cancelled and
		// the cancel handler is installed. The cancelHandler sends a PostgreSQL
		// cancel request without sleeping; the cancel may still be in transit
		// when fn returns. Polling SELECT 1 absorbs any stale 57014
		// deterministically before pool release. Without the cancel handler
		// there is no in-flight CancelRequest to absorb, so we skip the drain.
		// If the connection is broken, GC it so the pool replaces it.
		if p.cancelHandlerInstalled && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			if conn != nil && !p.drainCancelledConnFn(conn) {
				p.GC(conn.Conn())
			}
		} else if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			log.Ctx(ctx).Warn().Err(err).Uint8("retries", retries).Msg("error is not resettable or retryable")
		}

		if conn != nil {
			conn.Release()
			conn = nil // suppress the deferred release
		}
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

	// A query-canceled (57014) with a live context means a stale CancelRequest
	// from a prior context cancellation reached the connection after drainConn
	// had already confirmed it clean (the net.Conn deadline fired and SELECT 1
	// succeeded before the in-flight cancel arrived at CRDB). The connection is
	// clean once CRDB returns 57014; retry on the same connection.
	if sqlErrorCode(err) == CrdbQueryCanceledCode {
		return &RetryableError{Err: err}
	}

	return err
}

// connQueryer is the subset of pgx.Conn used by drainConn, enabling unit testing
// without a real database connection.
type connQueryer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// drainCancelledConn polls SELECT 1 on conn until the connection is provably
// clean or broken, then returns whether the connection is safe to return to
// the pool.
//
// When a query is cancelled via the PostgreSQL cancel protocol, a cancel
// request may still be in transit to CRDB after the calling goroutine receives
// the context error. If that in-flight cancel arrives on the connection's next
// query, CRDB returns SQLSTATE 57014. This function absorbs any such stragglers
// deterministically.
//
// Returns false if the connection should be GC'd.
func drainCancelledConn(conn *pgxpool.Conn) bool {
	return drainConn(conn.Conn())
}

// drainConn is the testable core of drainCancelledConn. It uses a 5-second
// timeout: generous enough to handle a briefly-unresponsive CRDB node, yet
// bounded so a partitioned node doesn't hold a pool connection indefinitely.
// In practice only 0–2 iterations occur (the cancel either already hit the
// original query or arrives on the first SELECT 1).
func drainConn(q connQueryer) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return drainConnWithContext(ctx, q)
}

// drainConnWithContext is the inner loop, exposed for testing with custom timeouts.
// A 5ms sleep between 57014 iterations avoids tight-spinning if CRDB is slow to
// process the cancel; in practice only 0–2 iterations occur.
func drainConnWithContext(ctx context.Context, q connQueryer) bool {
	for {
		_, err := q.Exec(ctx, "SELECT 1")
		if err == nil {
			return true // clean — no cancel outstanding
		}
		if sqlErrorCode(err) == CrdbQueryCanceledCode {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(5 * time.Millisecond):
			}
			continue // cancel absorbed, loop to confirm clean
		}
		return false // connection broken or timed out
	}
}
