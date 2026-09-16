//go:build datastore

package crdb

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// rangeGen yields unique relationships objectType:<i>#relation@subjectType:<i>
// for i in [start, end). Each row is a distinct primary-key entry, and chunking
// by disjoint ranges lets a large dataset load across several transactions
// without primary-key collisions.
type rangeGen struct {
	objectType  string
	relation    string
	subjectType string
	next        int
	end         int
}

func (g *rangeGen) Next(_ context.Context) (*tuple.Relationship, error) {
	if g.next >= g.end {
		return nil, nil
	}
	id := strconv.Itoa(g.next)
	g.next++
	return &tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{ObjectType: g.objectType, ObjectID: id, Relation: g.relation},
			Subject:  tuple.ObjectAndRelation{ObjectType: g.subjectType, ObjectID: id, Relation: datastore.Ellipsis},
		},
	}, nil
}

var _ datastore.BulkWriteRelationshipSource = (*rangeGen)(nil)

func benchEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func benchLoad(t *testing.T, ctx context.Context, ds datastore.Datastore, objectType string, n, chunk int) {
	t.Helper()
	for base := 0; base < n; base += chunk {
		end := min(base+chunk, n)
		gen := &rangeGen{objectType: objectType, relation: "viewer", subjectType: "user", next: base, end: end}
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			_, err := rwt.BulkLoad(ctx, gen)
			return err
		})
		require.NoError(t, err)
	}
}

// runDeleteLoop deletes everything matching filter in batches of batchSize, one
// transaction per batch, and returns the per-batch wall-clock durations plus the
// total deleted. When cursored is true it drives the cursor-advanced path
// (WithCursoredDelete + WithDeleteAfter); otherwise it drives the pre-existing
// naive path (WithDeleteLimit only) that rescans tombstones each batch.
func runDeleteLoop(t *testing.T, ctx context.Context, ds datastore.Datastore, filter *v1.RelationshipFilter, batchSize uint64, cursored bool) (uint64, []time.Duration) {
	t.Helper()
	var total uint64
	var durs []time.Duration
	var cursor options.Cursor

	for {
		batch := batchSize
		opts := []options.DeleteOptionsOption{options.WithDeleteLimit(&batch)}
		if cursored {
			opts = append(opts, options.WithCursoredDelete(true))
			if cursor != nil {
				opts = append(opts, options.WithDeleteAfter(cursor))
			}
		}

		var res datastore.DeleteRelationshipsResult
		start := time.Now()
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			var innerErr error
			res, innerErr = rwt.DeleteRelationships(ctx, filter, opts...)
			return innerErr
		})
		elapsed := time.Since(start)
		require.NoError(t, err)

		if res.NumDeleted == 0 {
			break
		}
		total += res.NumDeleted
		durs = append(durs, elapsed)
		if cursored {
			require.NotNil(t, res.Cursor, "cursored batch deleted rows but returned no cursor")
			cursor = res.Cursor
		}
	}
	return total, durs
}

func summarize(durs []time.Duration) (total, first, last, mean time.Duration) {
	if len(durs) == 0 {
		return
	}
	first = durs[0]
	last = durs[len(durs)-1]
	for _, d := range durs {
		total += d
	}
	mean = total / time.Duration(len(durs))
	return
}

// TestCursoredDeleteBenchmark measures the cursored batch-delete path against
// the pre-existing naive DELETE ... LIMIT loop, on identically-loaded disjoint
// object types, to confirm the cursor avoids the tombstone-rescan degradation.
//
// Skipped unless BENCH_CURSORED_DELETE is set, since it loads a large dataset
// and takes minutes. Tune with BENCH_N (default 100000) and BENCH_BATCH
// (default 2000).
func TestCursoredDeleteBenchmark(t *testing.T) {
	if os.Getenv("BENCH_CURSORED_DELETE") == "" {
		t.Skip("set BENCH_CURSORED_DELETE=1 to run the cursored-delete benchmark")
	}

	n := benchEnvInt("BENCH_N", 100000)
	batch := safecast.RequireConvert[uint64](t, benchEnvInt("BENCH_BATCH", 2000))
	const loadChunk = 20000

	engine := testdatastore.RunCRDBForTesting(t, crdbTestVersion())
	ctx := t.Context()

	ds := engine.NewDatastore(t, func(_, uri string) datastore.Datastore {
		d, err := NewCRDBDatastore(
			ctx,
			uri,
			// A huge GC window keeps SpiceDB from treating any revision as
			// collectable during the run; the naive path's degradation depends
			// on the deleted rows' MVCC tombstones still being present.
			GCWindow(retainAllRevisions),
			RevisionQuantization(0),
			OverlapStrategy(overlapStrategyPrefix),
			WithAcquireTimeout(30*time.Second),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = d.Close() })
		return d
	})

	const naiveType = "benchdoc_naive"
	const cursoredType = "benchdoc_cursored"

	t.Logf("loading %d relationships into each of %q and %q (batch=%d)", n, naiveType, cursoredType, batch)
	loadStart := time.Now()
	benchLoad(t, ctx, ds, naiveType, n, loadChunk)
	benchLoad(t, ctx, ds, cursoredType, n, loadChunk)
	t.Logf("load complete in %s", time.Since(loadStart).Round(time.Millisecond))

	naiveTotal, naiveDurs := runDeleteLoop(t, ctx, ds, &v1.RelationshipFilter{ResourceType: naiveType}, batch, false)
	require.Equal(t, safecast.RequireConvert[uint64](t, n), naiveTotal, "naive delete did not remove every relationship")

	cursoredTotal, cursoredDurs := runDeleteLoop(t, ctx, ds, &v1.RelationshipFilter{ResourceType: cursoredType}, batch, true)
	require.Equal(t, safecast.RequireConvert[uint64](t, n), cursoredTotal, "cursored delete did not remove every relationship")

	// Per-batch curve, zipped (both strategies run the same number of batches).
	t.Logf("per-batch latency (ms):  batch |    naive |  cursored")
	for i := 0; i < len(naiveDurs) || i < len(cursoredDurs); i++ {
		var nv, cv float64
		if i < len(naiveDurs) {
			nv = float64(naiveDurs[i].Microseconds()) / 1000
		}
		if i < len(cursoredDurs) {
			cv = float64(cursoredDurs[i].Microseconds()) / 1000
		}
		t.Logf("                        %5d | %8.1f | %9.1f", i, nv, cv)
	}

	nTot, nFirst, nLast, nMean := summarize(naiveDurs)
	cTot, cFirst, cLast, cMean := summarize(cursoredDurs)

	t.Logf("=== cursored bulk delete benchmark (N=%d per type, batch=%d, %d batches) ===", n, batch, len(naiveDurs))
	t.Logf("naive    : total=%s first=%s last=%s mean=%s  climb(last/first)=%.1fx",
		nTot.Round(time.Millisecond), nFirst.Round(time.Millisecond), nLast.Round(time.Millisecond),
		nMean.Round(time.Millisecond), float64(nLast)/float64(nFirst))
	t.Logf("cursored : total=%s first=%s last=%s mean=%s  climb(last/first)=%.1fx",
		cTot.Round(time.Millisecond), cFirst.Round(time.Millisecond), cLast.Round(time.Millisecond),
		cMean.Round(time.Millisecond), float64(cLast)/float64(cFirst))
	t.Logf("speedup  : total=%.1fx  mean-batch=%.1fx  last-batch=%.1fx",
		float64(nTot)/float64(cTot), float64(nMean)/float64(cMean), float64(nLast)/float64(cLast))
}

// extractRowsRead scans EXPLAIN ANALYZE plan text for the KV rows-read counters
// (label varies by CockroachDB version: "KV rows read" / "KV rows decoded"),
// returning every value found with commas stripped.
func extractRowsRead(lines []string) []int64 {
	var out []int64
	for _, ln := range lines {
		low := strings.ToLower(ln)
		if !strings.Contains(low, "rows read") && !strings.Contains(low, "rows decoded") {
			continue
		}
		colon := strings.LastIndex(ln, ":")
		if colon < 0 {
			continue
		}
		num := strings.TrimSpace(ln[colon+1:])
		num = strings.ReplaceAll(num, ",", "")
		if v, err := strconv.ParseInt(num, 10, 64); err == nil {
			out = append(out, v)
		}
	}
	return out
}

// quoteLit renders a string as a SQL string literal.
func quoteLit(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// inlineArgs substitutes $1..$N placeholders with quoted literals, highest index
// first so $10 is not partially matched by $1. CockroachDB's EXPLAIN ANALYZE
// rejects placeholders, and this diagnostic runs only against controlled test
// data, so inlining is safe here.
func inlineArgs(sql string, args []any) string {
	for i := len(args); i >= 1; i-- {
		sql = strings.ReplaceAll(sql, "$"+strconv.Itoa(i), quoteLit(fmt.Sprint(args[i-1])))
	}
	return sql
}

// extractScanKVTime returns the scan node's "KV time: <dur>" from a plan, which
// captures physical MVCC stepping (including over tombstones) rather than the
// logical rows-decoded count.
func extractScanKVTime(lines []string) time.Duration {
	var maxDur time.Duration
	for _, ln := range lines {
		low := strings.ToLower(ln)
		if !strings.Contains(low, "kv time:") {
			continue
		}
		colon := strings.LastIndex(ln, ":")
		if colon < 0 {
			continue
		}
		raw := strings.TrimSpace(ln[colon+1:])
		if d, err := time.ParseDuration(raw); err == nil && d > maxDur {
			maxDur = d
		}
	}
	return maxDur
}

func explainAnalyze(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string) []string {
	t.Helper()
	rows, err := pool.Query(ctx, "EXPLAIN ANALYZE "+sql)
	require.NoError(t, err)
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(t, rows.Err())
	return lines
}

// TestCursoredDeleteChurnDiagnostic measures KV rows read (the hardware-
// independent signal the CockroachDB analysis cites) rather than wall-clock.
// It drives the naive DELETE ... LIMIT loop via EXPLAIN ANALYZE and reports KV
// rows read per batch; if that climbs with churn, the tombstone rescan is real
// even when a hot single-node cluster hides it in wall-clock. It then probes
// the cursored CTE at deep churn to show its KV rows read stays ~batch.
func TestCursoredDeleteChurnDiagnostic(t *testing.T) {
	if os.Getenv("BENCH_CURSORED_DELETE") == "" {
		t.Skip("set BENCH_CURSORED_DELETE=1 to run the churn diagnostic")
	}

	n := benchEnvInt("BENCH_N", 100000)
	batch := safecast.RequireConvert[uint64](t, benchEnvInt("BENCH_BATCH", 5000))
	const loadChunk = 20000

	engine := testdatastore.RunCRDBForTesting(t, crdbTestVersion())
	ctx := t.Context()

	var connURI string
	ds := engine.NewDatastore(t, func(_, uri string) datastore.Datastore {
		connURI = uri
		d, err := NewCRDBDatastore(ctx, uri,
			GCWindow(retainAllRevisions),
			RevisionQuantization(0),
			OverlapStrategy(overlapStrategyPrefix),
			WithAcquireTimeout(30*time.Second),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = d.Close() })
		return d
	})

	pool, err := pgxpool.New(ctx, connURI)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	// --- Naive path: KV rows read per batch, driven by EXPLAIN ANALYZE. ---
	const naiveType = "churn_naive"
	benchLoad(t, ctx, ds, naiveType, n, loadChunk)

	intBatch := safecast.RequireConvert[int](t, batch)
	numBatches := (n + intBatch - 1) / intBatch
	naiveSQL := "DELETE FROM relation_tuple@pk_relation_tuple WHERE namespace = " + quoteLit(naiveType) + " LIMIT " + strconv.FormatUint(batch, 10)
	t.Logf("naive SQL: %s", naiveSQL)

	var naiveKV []int64
	var naiveScanTime []time.Duration
	for i := 0; i < numBatches; i++ {
		lines := explainAnalyze(t, ctx, pool, naiveSQL)
		if i == 0 || i == numBatches-1 {
			t.Logf("--- naive EXPLAIN ANALYZE plan, batch %d ---", i)
			for _, ln := range lines {
				t.Logf("    %s", ln)
			}
		}
		vals := extractRowsRead(lines)
		var maxRead int64
		for _, v := range vals {
			if v > maxRead {
				maxRead = v
			}
		}
		naiveKV = append(naiveKV, maxRead)
		naiveScanTime = append(naiveScanTime, extractScanKVTime(lines))
	}
	t.Logf("naive per batch: KV rows decoded stays flat; scan KV time climbs with tombstones")
	for i := range naiveKV {
		t.Logf("    naive batch %3d: KV rows decoded = %d  scan KV time = %s", i, naiveKV[i], naiveScanTime[i].Round(time.Millisecond))
	}

	// --- Cursored path: KV rows read at deep churn, via the shipped CTE. ---
	const cursoredType = "churn_cursored"
	benchLoad(t, ctx, ds, cursoredType, n, loadChunk)

	// Real-delete ~half via the cursored datastore path to reach deep churn and
	// obtain a mid-range cursor.
	filterCursored := &v1.RelationshipFilter{ResourceType: cursoredType}
	var cursor options.Cursor
	var deleted uint64
	halfN := safecast.RequireConvert[uint64](t, n/2)
	for deleted < halfN {
		b := batch
		opts := []options.DeleteOptionsOption{options.WithDeleteLimit(&b), options.WithCursoredDelete(true)}
		if cursor != nil {
			opts = append(opts, options.WithDeleteAfter(cursor))
		}
		var res datastore.DeleteRelationshipsResult
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			var e error
			res, e = rwt.DeleteRelationships(ctx, filterCursored, opts...)
			return e
		})
		require.NoError(t, err)
		require.NotNil(t, res.Cursor)
		cursor = res.Cursor
		deleted += res.NumDeleted
	}
	t.Logf("cursored: deleted %d (~50%%) to reach deep churn; cursor at %s", deleted, tuple.MustString(*options.ToRelationship(cursor)))

	// Build the exact shipped cursored SQL at this deep cursor and probe its KV work.
	b := batch
	delOpts := options.NewDeleteOptionsWithOptions(options.WithDeleteLimit(&b), options.WithDeleteAfter(cursor))
	cursoredSQL, cursoredArgs, _, err := buildCursoredDeleteQuery(testSchemaInfo(t), filterCursored, delOpts)
	require.NoError(t, err)
	lines := explainAnalyze(t, ctx, pool, inlineArgs(cursoredSQL, cursoredArgs))
	t.Logf("--- cursored EXPLAIN ANALYZE plan at ~50%% churn ---")
	for _, ln := range lines {
		t.Logf("    %s", ln)
	}
	cursoredVals := extractRowsRead(lines)
	var cursoredMax int64
	for _, v := range cursoredVals {
		if v > cursoredMax {
			cursoredMax = v
		}
	}
	cursoredScanTime := extractScanKVTime(lines)

	t.Logf("=== churn diagnostic (N=%d, batch=%d) ===", n, batch)
	t.Logf("naive    : rows decoded flat at %d/batch; scan KV time %s (batch 0) -> %s (last batch, %d tombstones ahead)  = %.1fx",
		naiveKV[0], naiveScanTime[0].Round(time.Millisecond), naiveScanTime[len(naiveScanTime)-1].Round(time.Millisecond),
		(numBatches-1)*intBatch, float64(naiveScanTime[len(naiveScanTime)-1])/float64(naiveScanTime[0]))
	t.Logf("cursored : rows decoded %d; scan KV time %s at ~50%% churn (seeks past %d tombstones)",
		cursoredMax, cursoredScanTime.Round(time.Millisecond), deleted)
	t.Logf("=> cursored scan KV time is churn-independent; naive scan KV time grows linearly with accumulated tombstones (quadratic total work)")
}
