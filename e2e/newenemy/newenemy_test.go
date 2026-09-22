package newenemy

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/e2e"
	"github.com/authzed/spicedb/e2e/cockroach"
	"github.com/authzed/spicedb/e2e/generator"
	"github.com/authzed/spicedb/e2e/spice"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

type NamespaceNames struct {
	Allowlist string
	Blocklist string
	User      string
	Resource  string
}

type SchemaData struct {
	Namespaces []NamespaceNames
}

const (
	schemaText = `
{{ range .Namespaces }}

definition {{.User}} {}

definition {{.Blocklist}} {
   relation user: {{.User}}
}

definition {{.Allowlist}} {
   relation user: {{.User}}
}

definition {{.Resource}} {
	relation direct: {{.Allowlist}}
	relation excluded: {{.Blocklist}}
	permission allowed = direct->user - excluded->user
}
{{ end }}
`
	objIDRegex           = "[a-zA-Z0-9_][a-zA-Z0-9/_-]{0,127}"
	namespacePrefixRegex = "[a-z][a-z0-9_]{1,62}[a-z0-9]"
)

var (
	maxIterations = flag.Int("max-iterations", 1000, "iteration cap for statistic-based tests (0 for no limit)")

	schemaTpl       = template.Must(template.New("schema").Parse(schemaText))
	prefixGenerator = generator.NewUniqueGenerator(namespacePrefixRegex)

	testCtx context.Context
)

func TestMain(m *testing.M) {
	var cancel context.CancelFunc
	testCtx, cancel = context.WithCancel(e2e.Context())
	code := m.Run()
	cancel()
	os.Exit(code)
}

const (
	createDB       = "CREATE DATABASE %s;"
	setSmallRanges = "ALTER DATABASE %s CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 65536, num_replicas = 1, gc.ttlseconds = 10;"
	dbName         = "spicedbnetest"
)

// fillWriteTimeout bounds a single write issued by the setup phases: one
// WriteSchema batch, or one WriteRelationships batch.
//
// These writes are small and fast on a healthy cluster -- tens of milliseconds
// each, with the slowest ever seen in a passing CI run under a second -- so
// this bound is not meant to be tight. It is meant to be finite. Without it a
// write inherits only the suite's own deadline, and this test deliberately
// puts CockroachDB into a state where a write can stop making progress
// altogether:
//
//	ALTER DATABASE ... range_max_bytes = 65536, num_replicas = 1, gc.ttlseconds = 10
//
// splits namespace_config into many tiny single-replica ranges. SpiceDB's
// WriteSchema reads the whole of that table inside its read-write
// transaction, so once the table is large and the cluster is still busy
// splitting and down-replicating, that scan can take ten seconds or more. A
// read span that wide cannot be refreshed, so the commit fails
// RETRY_SERIALIZABLE every time and SpiceDB retries it, with backoff, forever.
//
// Observed in CI: a WriteSchema stuck in exactly that loop for 29 minutes,
// re-reading namespace_config in 12.48s each time and failing the commit on
// every one of 20 attempts, until the go test deadline killed the run. The
// test reported nothing for those 29 minutes and the job burned half an hour
// of a large runner. Bounding the individual write turns that into a prompt,
// legible failure that names the write that stalled.
const fillWriteTimeout = 90 * time.Second

// namespaceSearchTimeout bounds namespacesForNode's search.
//
// The search is a guess-and-check loop: each attempt writes one more schema
// and asks CockroachDB where the resulting ranges landed, looking for a
// quadruple of namespaces whose lease holders happen to sit on the right
// nodes. How many attempts that takes is purely a matter of luck -- passing CI
// runs have needed anywhere from 48 to 646 of them, the latter taking a little
// over two minutes -- so the bound is deliberately generous. Its job is to
// stop the loop spinning against a cluster that has stopped accepting schema
// writes at all, which is where the stall above was found.
const namespaceSearchTimeout = 5 * time.Minute

// withFillTimeout derives the bounded context for a single setup write. See
// fillWriteTimeout.
func withFillTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, fillWriteTimeout)
}

func initializeTestCRDBCluster(ctx context.Context, t testing.TB) cockroach.Cluster {
	require := require.New(t)
	tlog := e2e.NewTLog(t)

	t.Log("starting cockroach...")
	crdbCluster := cockroach.NewCluster(3)
	for _, c := range crdbCluster {
		require.NoError(c.Start(ctx))
	}
	t.Cleanup(func() {
		require.NoError(crdbCluster.Stop(tlog))
	})

	t.Log("initializing crdb...")

	crdbCluster.Init(ctx, tlog, tlog)
	require.NoError(crdbCluster.SQL(ctx, tlog, tlog,
		"SET CLUSTER SETTING kv.range.backpressure_range_size_multiplier=0;",
	))
	require.NoError(crdbCluster.SQL(ctx, tlog, tlog,
		fmt.Sprintf(createDB, dbName),
	))

	t.Log("migrating...")
	require.NoError(spice.MigrateHead(ctx, tlog, "cockroachdb", crdbCluster[0].ConnectionString(dbName)))

	t.Log("attempting to connect...")
	require.NoError(crdbCluster[2].Connect(ctx, tlog, dbName))
	require.NoError(crdbCluster[1].Connect(ctx, tlog, dbName))
	require.NoError(crdbCluster[0].Connect(ctx, tlog, dbName))

	return crdbCluster
}

func TestNoNewEnemy(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	ctx, cancel := context.WithCancel(testCtx)
	t.Cleanup(cancel)

	// stop execution before the deadline (if one is set) to let cleanup run
	deadline, ok := t.Deadline()
	if ok {
		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-1*time.Minute))
		t.Cleanup(cancel)
		defer func() {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.FailNow()
			}
		}()
	}

	crdb := initializeTestCRDBCluster(ctx, t)

	tlog := e2e.NewTLog(t)

	t.Log("starting vulnerable spicedb...")
	vulnerableSpiceDB := spice.NewClusterFromCockroachCluster(crdb, spice.WithDBName(dbName))
	require.NoError(t, vulnerableSpiceDB.Start(ctx, tlog, "vulnerable",
		"--datastore-tx-overlap-strategy=insecure"))
	require.NoError(t, vulnerableSpiceDB.Connect(ctx, tlog))
	t.Cleanup(func() {
		require.NoError(t, vulnerableSpiceDB.Stop(tlog))
	})

	t.Log("start protected spicedb cluster")
	requestProtectedSpiceDB := spice.NewClusterFromCockroachCluster(crdb,
		spice.WithGrpcPort(50061),
		spice.WithDispatchPort(50062),
		spice.WithHTTPPort(8444),
		spice.WithMetricsPort(9100),
		spice.WithDBName(dbName))
	require.NoError(t, requestProtectedSpiceDB.Start(ctx, tlog, "reqprotected",
		"--datastore-tx-overlap-strategy=request"))
	require.NoError(t, requestProtectedSpiceDB.Connect(ctx, tlog))
	t.Cleanup(func() {
		require.NoError(t, requestProtectedSpiceDB.Stop(tlog))
	})

	standardProtectedSpiceDB := spice.NewClusterFromCockroachCluster(crdb,
		spice.WithGrpcPort(50071),
		spice.WithDispatchPort(50072),
		spice.WithHTTPPort(8454),
		spice.WithMetricsPort(9200),
		spice.WithDBName(dbName))
	require.NoError(t, standardProtectedSpiceDB.Start(ctx, tlog, "stdprotected",
		"--datastore-tx-overlap-strategy=static"))
	require.NoError(t, standardProtectedSpiceDB.Connect(ctx, tlog))
	t.Cleanup(func() {
		require.NoError(t, standardProtectedSpiceDB.Stop(tlog))
	})

	t.Log("configure small ranges, single replicas, short ttl")
	require.NoError(t, crdb.SQL(ctx, tlog, tlog,
		fmt.Sprintf(setSmallRanges, dbName),
	))

	t.Log("fill with schemas to span multiple ranges")
	// 4000 is larger than we need to span all three nodes, but a higher number
	// seems to make the test converge faster
	schemaData := generateSchemaData(4000, 500)
	fillSchema(ctx, t, schemaTpl, schemaData, vulnerableSpiceDB[1].Client().V1().Schema())
	slowNodeID, err := crdb[1].NodeID(ctx)
	require.NoError(t, err)
	prefix := namespacesForNode(ctx, t, crdb[1].Conn(), vulnerableSpiceDB[0].Client().V1().Schema(), slowNodeID)
	t.Log("fill with relationships to span multiple ranges")
	fill(ctx, t, vulnerableSpiceDB[0].Client().V1().Permissions(), prefix, generator.NewUniqueGenerator(objIDRegex), 500, 500)

	t.Log("modifying time")
	require.NoError(t, crdb.TimeDelay(ctx, e2e.MustFile(ctx, t, "timeattack-1.log"), 1, -200*time.Millisecond))

	tests := []struct {
		name            string
		vulnerableProbe probeFn
		protectedProbe  probeFn
		vulnerableMax   int
		sampleSize      int
	}{
		{
			name: "protected from data newenemy with standard overlap",
			vulnerableProbe: func(count int) (bool, int) {
				return checkDataNoNewEnemy(ctx, t, slowNodeID, crdb, vulnerableSpiceDB, count, true)
			},
			protectedProbe: func(count int) (bool, int) {
				return checkDataNoNewEnemy(ctx, t, slowNodeID, crdb, standardProtectedSpiceDB, count, false)
			},
			vulnerableMax: 20,
			sampleSize:    5,
		},
		{
			name: "protected from data newenemy per request",
			vulnerableProbe: func(count int) (bool, int) {
				return checkDataNoNewEnemy(ctx, t, slowNodeID, crdb, vulnerableSpiceDB, count, true)
			},
			protectedProbe: func(count int) (bool, int) {
				return checkDataNoNewEnemy(ctx, t, slowNodeID, crdb, requestProtectedSpiceDB, count, false)
			},
			vulnerableMax: 20,
			sampleSize:    5,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			vulnerableFn, protectedFn := attemptFnsForProbeFns(tt.vulnerableMax, tt.vulnerableProbe, tt.protectedProbe)
			statTest(t, tt.sampleSize, vulnerableFn, protectedFn)
		})
	}
}

// probeFn tests a condition a maximum of n times, returning whether the
// condition holds and how many times it was attempted. It is used as a building
// block for attemptFn and protectedAttemptFn.
type probeFn func(n int) (success bool, count int)

// attemptFnsForProbeFns takes probeFns and turns them into "testFns" that are used
// by stat tests
func attemptFnsForProbeFns(vulnerableMax int, vulnerableProbe, protectedProbe probeFn) (vulnerableFn attemptFn, protectedFn protectedAttemptFn) {
	vulnerableFn = func(t *testing.T) int {
		protected := true
		var attempts int
		for protected {
			// attempt vulnerableMax times before resetting
			// this helps if the test gets stuck in a state with bad initial
			// conditions, like a prefix that never lands on the right nodes
			protected, attempts = vulnerableProbe(vulnerableMax)
		}
		require.False(t, protected)
		t.Logf("determined spicedb vulnerable in %d attempts", attempts)
		return attempts
	}
	protectedFn = func(t *testing.T, count int) {
		t.Logf("check spicedb is protected after %d attempts", count)
		protected := true
		var attempts int
		for protected {
			protected, attempts = protectedProbe(count)
			// if the number of attempts doesn't match the count, that means
			// the test has requests a reset for some reason.
			if attempts < count {
				continue
			}
			require.True(t, protected, "protection is enabled, but newenemy detected")
			require.Equal(t, count, attempts)
			t.Logf("spicedb is protected after %d attempts", count)
			return
		}
	}
	return vulnerableFn, protectedFn
}

// attemptFn runs a check and returns how many iterations it took to fail
type attemptFn func(t *testing.T) int

// protectedAttemptFn runs a check and fails the current test if it fails within
// `count` iterations
type protectedAttemptFn func(t *testing.T, count int)

// statTest takes a testFn that is expected to fail the test, returning a sample
// and a protectedTestFn that is expected to succeed even after a given number
// of runs.
func statTest(t *testing.T, sampleSize int, vulnerableFn attemptFn, protectedFn protectedAttemptFn) {
	samples := make([]int, sampleSize)
	for i := 0; i < sampleSize; i++ {
		t.Logf("collecting sample %d", i)
		samples[i] = vulnerableFn(t)
	}
	protectedFn(t, iterationsForHighConfidence(samples))
}

// iterationsForHighConfidence returns how many iterations we need to get
// > 3sigma from the mean of the samples.
// Caps at maxIterations to control test runtime. Set maxIterations to 0 for no
// cap.
func iterationsForHighConfidence(samples []int) (iterations int) {
	// from https://cs.opensource.google/go/x/perf/+/40a54f11:internal/stats/sample.go;l=196
	// calculates mean and stddev at the same time
	mean, M2 := 0.0, 0.0
	for n, x := range samples {
		delta := float64(x) - mean
		mean += delta / float64(n+1)
		M2 += delta * (float64(x) - mean)
	}
	variance := M2 / float64(len(samples)-1)
	stddev := math.Sqrt(variance)

	samplestddev := stddev / math.Sqrt(float64(len(samples)))
	// how many iterations do we need to get > 3sigma from the mean?
	// cap maxIterations to control test runtime.
	iterations = int(math.Ceil(3*stddev*samplestddev + mean))
	if *maxIterations != 0 && *maxIterations < iterations {
		iterations = *maxIterations
	}
	return iterations
}

var goodNs *NamespaceNames

func checkDataNoNewEnemy(ctx context.Context, t testing.TB, slowNodeID int, crdb cockroach.Cluster, spicedb spice.Cluster, maxAttempts int, exitEarly bool) (bool, int) {
	objIDGenerator := generator.NewUniqueGenerator(objIDRegex)
	var prefix NamespaceNames
	if goodNs == nil {
		prefix = namespacesForNode(ctx, t, crdb[1].Conn(), spicedb[0].Client().V1().Schema(), slowNodeID)
		t.Log("filling with data to span multiple ranges for prefix", prefix)
		fill(ctx, t, spicedb[0].Client().V1().Permissions(), prefix, objIDGenerator, 100, 100)
	} else {
		// attempt to keep finding reversed timestamps in a namespace we've already seen them in
		prefix = *goodNs
	}
	allowlists, blocklists, allowusers, blockusers := generateTuples(prefix, maxAttempts, objIDGenerator)

	// write prereqs
	for i := 0; i < maxAttempts; i++ {
		_, err := spicedb[0].Client().V1().Permissions().WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: []*v1.RelationshipUpdate{allowusers[i], blocklists[i]},
		})
		require.NoError(t, err)
	}

	time.Sleep(500 * time.Millisecond)

	for i := 0; i < maxAttempts; i++ {
		ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
			string(requestmeta.RequestOverlapKey): fmt.Sprintf("test-%d", i),
		}))

		// exclude user by connecting user to blocklist
		r1, err := spicedb[0].Client().V1().Permissions().WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: []*v1.RelationshipUpdate{blockusers[i]},
		})
		require.NoError(t, err)
		t.Log("r1 token: ", r1.WrittenAt.Token)

		// sleeping in between the writes seems to let cockroach clear out
		// pending transactions in the background and has a better chance
		// of a clock desync on the second write
		sleep := (time.Duration(i) * 10) % 100 * time.Millisecond
		time.Sleep(sleep)

		// write to node 2 (clock is behind)
		// allow user by adding allowlist to resource
		r2, err := spicedb[1].Client().V1().Permissions().WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: []*v1.RelationshipUpdate{allowlists[i]},
		})
		require.NoError(t, err)
		t.Log("r2 token: ", r2.WrittenAt.Token)

		z1, _ := zedtoken.DecodeRevision(r1.WrittenAt, revisions.CommonDecoder{Kind: revisions.HybridLogicalClock})
		z2, _ := zedtoken.DecodeRevision(r2.WrittenAt, revisions.CommonDecoder{Kind: revisions.HybridLogicalClock})

		t.Log("z1 revision: ", z1.Revision)
		t.Log("z2 revision: ", z2.Revision)

		canHas, err := spicedb[1].Client().V1().Permissions().CheckPermission(context.Background(), &v1.CheckPermissionRequest{
			Consistency: &v1.Consistency{Requirement: &v1.Consistency_AtExactSnapshot{AtExactSnapshot: r2.WrittenAt}},
			Resource: &v1.ObjectReference{
				ObjectType: allowlists[i].Relationship.Resource.ObjectType,
				ObjectId:   allowlists[i].Relationship.Resource.ObjectId,
			},
			Permission: "allowed",
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: allowusers[i].Relationship.Subject.Object.ObjectType,
					ObjectId:   allowusers[i].Relationship.Subject.Object.ObjectId,
				},
			},
		})
		require.NoError(t, err)

		ns1BlocklistLeader := getLeaderNodeForNamespace(ctx, t, crdb[2].Conn(), blockusers[i].Relationship.Resource.ObjectType)
		ns1UserLeader := getLeaderNodeForNamespace(ctx, t, crdb[2].Conn(), blockusers[i].Relationship.Subject.Object.ObjectType)
		ns2ResourceLeader := getLeaderNodeForNamespace(ctx, t, crdb[2].Conn(), allowlists[i].Relationship.Resource.ObjectType)
		ns2AllowlistLeader := getLeaderNodeForNamespace(ctx, t, crdb[2].Conn(), allowlists[i].Relationship.Subject.Object.ObjectType)

		r1leader, r2leader := getLeaderNode(ctx, t, crdb[2].Conn(), blockusers[i].Relationship), getLeaderNode(ctx, t, crdb[2].Conn(), allowlists[i].Relationship)
		t.Log(sleep, z1, z2, z1.Revision.GreaterThan(z2.Revision), r1leader, r2leader, ns1BlocklistLeader, ns1UserLeader, ns2ResourceLeader, ns2AllowlistLeader)

		if z1.Revision.GreaterThan(z2.Revision) {
			t.Log("timestamps are reversed", canHas.Permissionship.String())
		}

		if canHas.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
			t.Log("service is subject to the new enemy problem")
			goodNs = &prefix
			return false, i + 1
		}

		if ns2ResourceLeader != slowNodeID || ns2AllowlistLeader != slowNodeID {
			t.Log("namespace leader changed, pick new namespaces and fill")
			goodNs = nil
			// returning true will re-run with a new prefix
			return true, i + 1
		}

		if r2leader == ns1UserLeader && i%5 == 4 && exitEarly {
			t.Log("second write to fast node, pick new namespaces")
			goodNs = nil
			return true, i + 1
		}

		if r1leader == ns2ResourceLeader && i%5 == 4 && exitEarly {
			goodNs = nil
			t.Log("first write to slow node, pick new namespaces")
			return true, i + 1
		}

		select {
		case <-ctx.Done():
			return false, i + 1
		default:
			// this sleep helps the clocks get back out of sync after an attempt
			time.Sleep(500 * time.Millisecond)
			continue
		}
	}
	return true, maxAttempts
}

func fill(ctx context.Context, t testing.TB, client v1.PermissionsServiceClient, prefix NamespaceNames, objIDGenerator *generator.UniqueGenerator, fillerCount, batchSize int) {
	t.Log("filling prefix", prefix)
	require := require.New(t)
	allowlists, blocklists, allowusers, blockusers := generateTuples(prefix, fillerCount, objIDGenerator)

	// Each batch is written under its own bounded context rather than directly
	// under ctx, whose only deadline is the suite's. See fillWriteTimeout.
	writeBatch := func(updates []*v1.RelationshipUpdate) error {
		writeCtx, cancel := withFillTimeout(ctx)
		defer cancel()

		_, err := client.WriteRelationships(writeCtx, &v1.WriteRelationshipsRequest{Updates: updates})
		return err
	}

	for i := 0; i < fillerCount/batchSize; i++ {
		t.Log("filling", i*batchSize, "to", (i+1)*batchSize)
		// Written in this order deliberately: this is a test about the order
		// in which writes land, so the batches keep the sequence they had
		// before each one gained its own context.
		for _, batch := range []struct {
			name    string
			updates []*v1.RelationshipUpdate
		}{
			{"allowlists", allowlists[i*batchSize : (i+1)*batchSize]},
			{"blocklists", blocklists[i*batchSize : (i+1)*batchSize]},
			{"allowusers", allowusers[i*batchSize : (i+1)*batchSize]},
			{"blockusers", blockusers[i*batchSize : (i+1)*batchSize]},
		} {
			require.NoErrorf(writeBatch(batch.updates),
				"writing %s relationships %d to %d did not complete within %s",
				batch.name, i*batchSize, (i+1)*batchSize, fillWriteTimeout)
		}
	}
}

// fillSchema generates the schema text for given SchemaData and applies it.
//
// Each batch is written under its own bounded context: these calls used to
// pass context.Background(), which meant they honoured neither the suite
// deadline nor any bound of their own. See fillWriteTimeout.
func fillSchema(ctx context.Context, t testing.TB, template *template.Template, data []SchemaData, schemaClient v1.SchemaServiceClient) {
	var b strings.Builder
	batchSize := len(data[0].Namespaces)
	for i, d := range data {
		t.Logf("filling %d to %d", i*batchSize, (i+1)*batchSize)
		b.Reset()
		require.NoError(t, template.Execute(&b, d))

		err := func() error {
			writeCtx, cancel := withFillTimeout(ctx)
			defer cancel()

			_, err := schemaClient.WriteSchema(writeCtx, &v1.WriteSchemaRequest{
				Schema: b.String(),
			})
			return err
		}()
		require.NoErrorf(t, err, "writing schema batch %d (namespaces %d to %d) did not complete within %s",
			i, i*batchSize, (i+1)*batchSize, fillWriteTimeout)
	}
}

// namespacesForNode finds a prefix with namespace leaders on the node id.
//
// The search has its own deadline (see namespaceSearchTimeout) rather than
// running until the suite's. It also fails the test when it gives up: the
// previous behaviour was to return a zero NamespaceNames, which the caller
// then used to fill relationships under empty namespace names, turning "the
// search never converged" into an unrelated-looking error several steps
// later.
func namespacesForNode(ctx context.Context, t testing.TB, conn *pgx.Conn, schemaClient v1.SchemaServiceClient, node int) NamespaceNames {
	searchCtx, cancelSearch := context.WithTimeout(ctx, namespaceSearchTimeout)
	defer cancelSearch()

	for attempt := 1; ; attempt++ {
		newSchema := generateSchemaData(1, 1)
		p := newSchema[0].Namespaces[0]
		var b strings.Builder
		require.NoError(t, schemaTpl.Execute(&b, newSchema[0]))

		err := func() error {
			writeCtx, cancel := withFillTimeout(searchCtx)
			defer cancel()

			_, err := schemaClient.WriteSchema(writeCtx, &v1.WriteSchemaRequest{
				Schema: b.String(),
			})
			return err
		}()
		// Either budget can be the one that expired here: the write's own, or
		// what is left of the search's. Name both rather than guess.
		require.NoErrorf(t, err,
			"writing the probe schema for attempt %d did not complete (per-write budget %s, search budget %s)",
			attempt, fillWriteTimeout, namespaceSearchTimeout)

		userLeader := getLeaderNodeForNamespace(searchCtx, t, conn, p.User)
		resourceLeader := getLeaderNodeForNamespace(searchCtx, t, conn, p.Resource)
		allowlistLeader := getLeaderNodeForNamespace(searchCtx, t, conn, p.Allowlist)
		blocklistLeader := getLeaderNodeForNamespace(searchCtx, t, conn, p.Blocklist)

		// allowlist and resource must be equal to node
		// blocklist and user must not be equal to node
		if blocklistLeader == userLeader && userLeader != node && allowlistLeader == resourceLeader && resourceLeader == node {
			fmt.Println("found namespaces", p.User, userLeader, p.Resource, resourceLeader, p.Allowlist, allowlistLeader, p.Blocklist, blocklistLeader)
			return p
		}

		select {
		case <-searchCtx.Done():
			t.Fatalf("gave up after %d attempts and %s looking for namespaces whose range leases land on node %d: %v",
				attempt, namespaceSearchTimeout, node, context.Cause(searchCtx))
			return NamespaceNames{}
		default:
			continue
		}
	}
}

func generateSchemaData(n int, batchSize int) (data []SchemaData) {
	data = make([]SchemaData, 0, n/batchSize)
	for i := 0; i < n/batchSize; i++ {
		schema := SchemaData{Namespaces: make([]NamespaceNames, 0, batchSize)}
		for j := i * batchSize; j < (i+1)*batchSize; j++ {
			schema.Namespaces = append(schema.Namespaces, NamespaceNames{
				User:      prefixGenerator.Next() + "user",
				Resource:  prefixGenerator.Next() + "resource",
				Allowlist: prefixGenerator.Next() + "allowlist",
				Blocklist: prefixGenerator.Next() + "blocklist",
			})
		}
		data = append(data, schema)
	}
	return data
}

func generateTuples(names NamespaceNames, n int, objIDGenerator *generator.UniqueGenerator) (allowlists []*v1.RelationshipUpdate, blocklists []*v1.RelationshipUpdate, allowusers []*v1.RelationshipUpdate, blockusers []*v1.RelationshipUpdate) {
	allowlists = make([]*v1.RelationshipUpdate, 0, n)
	blocklists = make([]*v1.RelationshipUpdate, 0, n)
	allowusers = make([]*v1.RelationshipUpdate, 0, n)
	blockusers = make([]*v1.RelationshipUpdate, 0, n)

	for i := 0; i < n; i++ {
		user := &v1.ObjectReference{
			ObjectType: names.User,
			ObjectId:   objIDGenerator.Next(),
		}

		allowlistID := objIDGenerator.Next()
		blocklistID := objIDGenerator.Next()

		tupleAllowuser := &v1.Relationship{
			Resource: &v1.ObjectReference{
				ObjectType: names.Allowlist,
				ObjectId:   allowlistID,
			},
			Relation: "user",
			Subject:  &v1.SubjectReference{Object: user},
		}

		tupleBlockuser := &v1.Relationship{
			Resource: &v1.ObjectReference{
				ObjectType: names.Blocklist,
				ObjectId:   blocklistID,
			},
			Relation: "user",
			Subject:  &v1.SubjectReference{Object: user},
		}

		tupleAllowlist := &v1.Relationship{
			Resource: &v1.ObjectReference{
				ObjectType: names.Resource,
				ObjectId:   "thegoods",
			},
			Relation: "direct",
			Subject: &v1.SubjectReference{Object: &v1.ObjectReference{
				ObjectType: names.Allowlist,
				ObjectId:   allowlistID,
			}},
		}

		tupleBlocklist := &v1.Relationship{
			Resource: &v1.ObjectReference{
				ObjectType: names.Resource,
				ObjectId:   "thegoods",
			},
			Relation: "excluded",
			Subject: &v1.SubjectReference{Object: &v1.ObjectReference{
				ObjectType: names.Blocklist,
				ObjectId:   blocklistID,
			}},
		}

		allowlists = append(allowlists, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: tupleAllowlist,
		})
		blocklists = append(blocklists, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: tupleBlocklist,
		})
		allowusers = append(allowusers, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: tupleAllowuser,
		})
		blockusers = append(blockusers, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: tupleBlockuser,
		})
	}
	return allowlists, blocklists, allowusers, blockusers
}

// getLeaderNode returns the node with the lease leader for the range containing the tuple
func getLeaderNode(ctx context.Context, t testing.TB, conn *pgx.Conn, tuple *v1.Relationship) int {
	row := conn.QueryRow(ctx, "SHOW RANGE FROM TABLE relation_tuple FOR ROW ($1::text,$2::text,$3::text,$4::text,$5::text,$6::text)",
		tuple.Resource.ObjectType,
		tuple.Resource.ObjectId,
		tuple.Relation,
		tuple.Subject.Object.ObjectType,
		tuple.Subject.Object.ObjectId,
		tuple.Subject.OptionalRelation,
	)

	return leaderFromRangeRow(t, row)
}

// getLeaderNodeForNamespace returns the node with the lease leader for the range containing the namespace
func getLeaderNodeForNamespace(ctx context.Context, t testing.TB, conn *pgx.Conn, namespace string) int {
	rows := conn.QueryRow(ctx, "SHOW RANGE FROM TABLE namespace_config FOR ROW ($1::text)",
		namespace,
	)
	return leaderFromRangeRow(t, rows)
}

// leaderFromRangeRow parses the rows from a `SHOW RANGE` query and returns the
// leader node id for the range.
//
// A scan failure fails the test rather than calling log.Fatal, which it used
// to do. log.Fatal here exits the test binary outright: no test output, no
// t.Cleanup, and so no Stop() for the three CockroachDB nodes and nine SpiceDB
// processes the suite started. That mattered little while the only way to get
// here with an error was a genuinely broken cluster, but the callers now pass
// contexts that can expire, and an expiring context must not leave a runner
// full of orphaned processes.
func leaderFromRangeRow(t testing.TB, row pgx.Row) int {
	var (
		startKey           sql.NullString
		endKey             sql.NullString
		rangeID            int
		leaseHolder        int
		leaseHoldeLocality sql.NullString
		replicas           []pgtype.Int8
		replicaLocalities  []pgtype.Text
	)

	err := row.Scan(&startKey, &endKey, &rangeID, &leaseHolder, &leaseHoldeLocality, &replicas, &replicaLocalities)
	require.NoError(t, err, "failed to load leader id")

	return leaseHolder
}
