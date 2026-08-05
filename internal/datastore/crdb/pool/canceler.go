package pool

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// defaultCancelTimeout bounds the full round trip of one CANCEL QUERIES
	// statement, including acquiring a connection from the cancel pool.
	defaultCancelTimeout = 2 * time.Second

	// defaultCancelDeadlineDelay is the fallback net.Conn deadline applied to
	// the original connection when its context is canceled. It must
	// comfortably exceed defaultCancelTimeout so the in-band cancellation has
	// time to resolve the in-flight query before the connection is destroyed.
	defaultCancelDeadlineDelay = 5 * time.Second

	// defaultCancelPoolMaxConns bounds concurrent CANCEL QUERIES statements.
	// Excess cancellations queue on the pool, applying natural backpressure;
	// queued past the statement timeout, they fail safe (the original
	// connection is destroyed, the legacy behavior).
	defaultCancelPoolMaxConns = 5
)

var (
	cancellationDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "crdb_query_cancellation_duration_seconds",
		Help:    "Round-trip time of in-band CANCEL QUERIES statements. This is the window during which the canceled request still holds its connection.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 13),
	})
	cancellationCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "crdb_query_cancellations_total",
		Help: "Number of in-band CANCEL QUERIES statements issued, by outcome. Outcome 'error' means the connection was discarded instead (legacy behavior).",
	}, []string{"outcome"})
)

func init() {
	prometheus.MustRegister(cancellationDuration, cancellationCounter)
}

// validSessionID matches CockroachDB session IDs as returned by
// `SHOW session_id` (a hex string). Validated at registration so the ID can
// be safely inlined into the CANCEL QUERIES statement.
var validSessionID = regexp.MustCompile(`^[0-9a-fA-F]+$`)

// Canceler cancels in-flight queries on tracked CockroachDB sessions using
// the in-band CANCEL QUERIES statement, issued over a small dedicated pool of
// sibling connections. Unlike the pgwire cancel protocol — which CockroachDB
// applies asynchronously after closing the cancel connection — CANCEL QUERIES
// completes only after the target query's context has been canceled on its
// gateway node, and it targets globally-unique query IDs, so it can never
// cancel the wrong query.
type Canceler struct {
	pool *pgxpool.Pool

	sync.RWMutex
	sessionIDs map[*pgconn.PgConn]string // GUARDED_BY(RWMutex)
}

// NewCanceler creates a Canceler with its own small connection pool derived
// from the given config. The config must come from pgxpool.ParseConfig.
func NewCanceler(ctx context.Context, config *pgxpool.Config) (*Canceler, error) {
	config = config.Copy()
	config.MinConns = 1
	config.MaxConns = defaultCancelPoolMaxConns

	p, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create cancel pool: %w", err)
	}
	return &Canceler{pool: p, sessionIDs: make(map[*pgconn.PgConn]string)}, nil
}

// RegisterSession captures the CockroachDB session ID of a connection so that
// its queries can later be canceled in-band. Intended to be called from
// pgxpool's AfterConnect hook (see InstallOn).
func (c *Canceler) RegisterSession(ctx context.Context, conn *pgx.Conn) error {
	var sessionID string
	if err := conn.QueryRow(ctx, "SHOW session_id").Scan(&sessionID); err != nil {
		return fmt.Errorf("unable to read session id: %w", err)
	}
	if !validSessionID.MatchString(sessionID) {
		return fmt.Errorf("unexpected session id format: %q", sessionID)
	}
	c.setSessionID(conn.PgConn(), sessionID)
	return nil
}

func (c *Canceler) setSessionID(pgConn *pgconn.PgConn, sessionID string) {
	c.Lock()
	defer c.Unlock()
	c.sessionIDs[pgConn] = sessionID
}

// UnregisterSession stops tracking a connection. Intended to be called from
// pgxpool's BeforeClose hook (see InstallOn).
func (c *Canceler) UnregisterSession(pgConn *pgconn.PgConn) {
	c.Lock()
	defer c.Unlock()
	delete(c.sessionIDs, pgConn)
}

// SessionID returns the tracked CockroachDB session ID for a connection.
func (c *Canceler) SessionID(pgConn *pgconn.PgConn) (string, bool) {
	c.RLock()
	defer c.RUnlock()
	id, ok := c.sessionIDs[pgConn]
	return id, ok
}

// CancelSessionQueries synchronously cancels all queries currently running on
// the session of the given connection. A nil return guarantees the CANCEL
// statement completed on the server: any query that was active on the session
// has had its context canceled (it will observe SQLSTATE 57014), and nothing
// remains in flight that could affect a future query on the session.
func (c *Canceler) CancelSessionQueries(pgConn *pgconn.PgConn) error {
	sessionID, ok := c.SessionID(pgConn)
	if !ok {
		return errors.New("no session registered for connection")
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultCancelTimeout)
	defer cancel()

	// The session ID is hex, validated at registration, and inlined rather
	// than bound as a placeholder for compatibility with the bracketed SHOW
	// subquery syntax.
	stmt := fmt.Sprintf(
		`CANCEL QUERIES IF EXISTS (SELECT query_id FROM [SHOW CLUSTER STATEMENTS] WHERE session_id = '%s')`,
		sessionID,
	)

	start := time.Now()
	_, err := c.pool.Exec(ctx, stmt)
	cancellationDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		cancellationCounter.WithLabelValues("error").Inc()
		return fmt.Errorf("unable to cancel queries for session %s: %w", sessionID, err)
	}
	cancellationCounter.WithLabelValues("ok").Inc()
	return nil
}

// InstallOn wires the canceler into a pool config: connections report their
// session IDs on connect, are untracked on close, and respond to context
// cancellation by canceling their queries in-band instead of being destroyed.
// Must be called before the config is used to construct a pool.
func (c *Canceler) InstallOn(config *pgxpool.Config) {
	afterConnect := config.AfterConnect
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if afterConnect != nil {
			if err := afterConnect(ctx, conn); err != nil {
				return err
			}
		}
		return c.RegisterSession(ctx, conn)
	}

	beforeClose := config.BeforeClose
	config.BeforeClose = func(conn *pgx.Conn) {
		if beforeClose != nil {
			beforeClose(conn)
		}
		c.UnregisterSession(conn.PgConn())
	}

	config.ConnConfig.BuildContextWatcherHandler = func(pgConn *pgconn.PgConn) ctxwatch.Handler {
		return NewCancelQueryContextWatcherHandler(pgConn, c, defaultCancelDeadlineDelay)
	}
}

// Close closes the cancel pool.
func (c *Canceler) Close() {
	c.pool.Close()
}
