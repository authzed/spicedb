package newenemy

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
	"text/template"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/spicedb/pkg/zookie"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/e2e"
	"github.com/authzed/spicedb/e2e/cockroach"
	"github.com/authzed/spicedb/e2e/generator"
	"github.com/authzed/spicedb/e2e/spice"
)

type SchemaData struct {
	Prefixes []string
}

const schemaText = `
{{ range .Prefixes }}
definition {{.}}/user {}
definition {{.}}/resource {
	relation direct: {{.}}/user
	relation excluded: {{.}}/user
	permission allowed = direct - excluded
}
{{ end }}
`

const (
	objIDRegex           = "[a-zA-Z0-9_][a-zA-Z0-9/_-]{0,127}"
	namespacePrefixRegex = "[a-z][a-z0-9_]{2,62}[a-z0-9]"
)

var (
	maxIterations = flag.Int("max-iterations", 1000, "iteration cap for statistic-based tests (0 for no limit)")

	schemaTpl = template.Must(template.New("schema").Parse(schemaText))
	testCtx   context.Context
)

func TestMain(m *testing.M) {
	var cancel context.CancelFunc
	testCtx, cancel = context.WithCancel(e2e.Context())
	code := m.Run()
	cancel()
	os.Exit(code)
}

const (
	createDb       = "CREATE DATABASE %s;"
	setSmallRanges = "ALTER DATABASE %s CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 65536, num_replicas = 1, gc.ttlseconds = 5;"
	dbName         = "spicedbnetest"
)

func initializeTestCRDBCluster(ctx context.Context, t testing.TB) cockroach.Cluster {
	require := require.New(t)

	t.Log("starting cockroach...")
	crdbCluster := cockroach.NewCluster(3)
	for _, c := range crdbCluster {
		require.NoError(c.Start(ctx))
	}

	t.Log("initializing crdb...")
	tlog := e2e.NewTLog(t)
	crdbCluster.Init(ctx, tlog, tlog)
	require.NoError(crdbCluster.SQL(ctx, tlog, tlog,
		fmt.Sprintf(createDb, dbName),
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
	require := require.New(t)
	rand.Seed(time.Now().UnixNano())
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	crdb := initializeTestCRDBCluster(ctx, t)

	tlog := e2e.NewTLog(t)

	t.Log("starting vulnerable spicedb...")
	vulnerableSpiceDb := spice.NewClusterFromCockroachCluster(crdb, spice.WithDbName(dbName))
	require.NoError(vulnerableSpiceDb.Start(ctx, tlog, "vulnerable",
		"--datastore-tx-overlap-strategy=insecure"))
	require.NoError(vulnerableSpiceDb.Connect(ctx, tlog))

	t.Log("start protected spicedb cluster")
	protectedSpiceDb := spice.NewClusterFromCockroachCluster(crdb,
		spice.WithGrpcPort(50061),
		spice.WithDispatchPort(50062),
		spice.WithHttpPort(8444),
		spice.WithMetricsPort(9100),
		spice.WithDashboardPort(8100),
		spice.WithDbName(dbName))
	require.NoError(protectedSpiceDb.Start(ctx, tlog, "protected"))
	require.NoError(protectedSpiceDb.Connect(ctx, tlog))

	t.Log("configure small ranges, single replicas, short ttl")
	require.NoError(crdb.SQL(ctx, tlog, tlog,
		fmt.Sprintf(setSmallRanges, dbName),
	))

	t.Log("fill with schemas to span multiple ranges")
	// 4000 is larger than we need to span all three nodes, but a higher number
	// seems to make the test converge faster
	schemaData := generateSchemaData(4000, 500)
	require.NoError(fillSchema(t, schemaData, vulnerableSpiceDb[1].Client().V1Alpha1().Schema()))

	t.Log("determining a prefix with a leader on the slow node")
	slowNodeId, err := crdb[1].NodeID(testCtx)
	require.NoError(err)
	slowPrefix := prefixForNode(ctx, crdb[1].Conn(), schemaData, slowNodeId)

	t.Logf("using prefix %s for slow node %d", slowPrefix, slowNodeId)

	t.Log("filling with data to span multiple ranges")
	fill(t, vulnerableSpiceDb[0].Client().V0().ACL(), slowPrefix, 4000, 1000)

	t.Log("modifying time")
	require.NoError(crdb.TimeDelay(ctx, e2e.MustFile(ctx, t, "timeattack.log"), 1, -200*time.Millisecond))

	const sampleSize = 5
	samples := make([]int, sampleSize)

	for i := 0; i < sampleSize; i++ {
		t.Log(i, "check vulnerability with mitigations disabled")
		checkCtx, checkCancel := context.WithTimeout(ctx, 30*time.Minute)
		protected, attempts := checkNoNewEnemy(checkCtx, t, schemaData, slowNodeId, crdb, vulnerableSpiceDb, -1)
		require.NotNil(protected, "unable to determine if spicedb displays newenemy when mitigations are disabled within the time limit")
		require.False(*protected)
		checkCancel()
		t.Logf("%d - determined spicedb vulnerable in %d attempts", i, attempts)
		samples[i] = attempts
	}

	// from https://cs.opensource.google/go/x/perf/+/40a54f11:internal/stats/sample.go;l=196
	// calculates mean and stddev at the same time
	mean, M2 := 0.0, 0.0
	for n, x := range samples {
		delta := float64(x) - mean
		mean += delta / float64(n+1)
		M2 += delta * (float64(x) - mean)
	}
	variance := M2 / float64(sampleSize-1)
	stddev := math.Sqrt(variance)

	samplestddev := stddev / math.Sqrt(float64(sampleSize))
	// how many iterations do we need to get > 3sigma from the mean?
	// cap maxIterations to control test runtime.
	iterations := int(math.Ceil(3*stddev*samplestddev + mean))
	if *maxIterations != 0 && *maxIterations < iterations {
		iterations = *maxIterations
	}

	t.Logf("check spicedb is protected after %d attempts", iterations)
	protected, _ := checkNoNewEnemy(ctx, t, schemaData, slowNodeId, crdb, protectedSpiceDb, iterations)
	require.NotNil(protected, "unable to determine if spicedb is protected within the time limit")
	require.True(*protected, "protection is enabled, but newenemy detected")
}

// checkNoNewEnemy returns true if the service is protected, false if it is vulnerable, and nil if we couldn't determine
func checkNoNewEnemy(ctx context.Context, t testing.TB, schemaData []SchemaData, slowNodeId int, crdb cockroach.Cluster, spicedb spice.Cluster, candidateCount int) (*bool, int) {
	var attempts int

	prefix := prefixForNode(ctx, crdb[1].Conn(), schemaData, slowNodeId)
	objIdGenerator := generator.NewUniqueGenerator(objIDRegex)

	for {
		attempts++
		directs, excludes := generateTuples(prefix, 1, objIdGenerator)

		// write to node 1
		r1, err := spicedb[0].Client().V0().ACL().Write(testCtx, &v0.WriteRequest{
			Updates: []*v0.RelationTupleUpdate{excludes[0]},
		})
		if err != nil {
			t.Log(err)
			continue
		}

		// the first write has to read the namespaces from the second node,
		// which will resync the timestamps. sleeping allows the clocks to get
		// back out of sync
		time.Sleep(100 * time.Millisecond)

		// write to node 2 (clock is behind)
		r2, err := spicedb[1].Client().V0().ACL().Write(testCtx, &v0.WriteRequest{
			Updates: []*v0.RelationTupleUpdate{directs[0]},
		})
		if err != nil {
			t.Log(err)
			continue
		}

		canHas, err := spicedb[1].Client().V0().ACL().Check(context.Background(), &v0.CheckRequest{
			TestUserset: &v0.ObjectAndRelation{
				Namespace: directs[0].Tuple.ObjectAndRelation.Namespace,
				ObjectId:  directs[0].Tuple.ObjectAndRelation.ObjectId,
				Relation:  "allowed",
			},
			User:       directs[0].Tuple.GetUser(),
			AtRevision: r2.GetRevision(),
		})
		if err != nil {
			t.Log(err)
			continue
		}
		if canHas.IsMember {
			t.Log("service is subject to the new enemy problem")
		}

		r1leader, r2leader := getLeaderNode(testCtx, crdb[1].Conn(), excludes[0].Tuple), getLeaderNode(testCtx, crdb[1].Conn(), directs[0].Tuple)
		ns1Leader := getLeaderNodeForNamespace(testCtx, crdb[1].Conn(), excludes[0].Tuple.ObjectAndRelation.Namespace)
		ns2Leader := getLeaderNodeForNamespace(testCtx, crdb[1].Conn(), excludes[0].Tuple.User.GetUserset().Namespace)
		z1, _ := zookie.DecodeRevision(r1.GetRevision())
		z2, _ := zookie.DecodeRevision(r2.GetRevision())
		t.Log(z1, z2, z1.Sub(z2).String(), r1leader, r2leader, ns1Leader, ns2Leader)

		if ns1Leader != slowNodeId || ns2Leader != slowNodeId {
			t.Log("namespace leader changed, pick new prefix and fill")
			prefix = prefixForNode(ctx, crdb[1].Conn(), schemaData, slowNodeId)
			// need to fill new prefix
			t.Log("filling with data to span multiple ranges for prefix ", prefix)
			fill(t, spicedb[0].Client().V0().ACL(), prefix, 4000, 1000)
			continue
		}

		if canHas.IsMember {
			t.Log("service is subject to the new enemy problem")
			protected := false
			return &protected, attempts
		}

		if !canHas.IsMember && z1.Sub(z2).IsPositive() {
			t.Log("error in test, object id has been re-used.")
			continue
		}

		// if we find causal reversals, but no newenemy, assume we're protected
		if candidateCount > 0 && attempts >= candidateCount {
			t.Log(candidateCount, "(would be) causal reversals with no new enemy detected")
			protected := true
			return &protected, attempts
		}

		if attempts > 1000 {
			t.Log("trying with a new prefix")
			attempts = 0
			prefix = prefixForNode(ctx, crdb[1].Conn(), schemaData, slowNodeId)
			t.Log("filling with data to span multiple ranges for prefix ", prefix)
			fill(t, spicedb[0].Client().V0().ACL(), prefix, 4000, 1000)
			continue
		}

		select {
		case <-ctx.Done():
			return nil, attempts
		default:
			continue
		}

		// allow clocks to desync
		time.Sleep(100 * time.Millisecond)
	}
}

func BenchmarkBatchWrites(b *testing.B) {
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	crdb := initializeTestCRDBCluster(ctx, b)
	spicedb := spice.NewClusterFromCockroachCluster(crdb, spice.WithDbName(dbName))
	tlog := e2e.NewTLog(b)
	require.NoError(b, spicedb.Start(ctx, tlog, ""))
	require.NoError(b, spicedb.Connect(ctx, tlog))

	directs, excludes := generateTuples("", b.N*20, generator.NewUniqueGenerator(objIDRegex))
	b.ResetTimer()
	_, err := spicedb[0].Client().V0().ACL().Write(testCtx, &v0.WriteRequest{
		Updates: excludes,
	})
	if err != nil {
		b.Log(err)
	}
	_, err = spicedb[0].Client().V0().ACL().Write(testCtx, &v0.WriteRequest{
		Updates: directs,
	})
	if err != nil {
		b.Log(err)
	}
}

func BenchmarkConflictingTupleWrites(b *testing.B) {
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	crdb := initializeTestCRDBCluster(ctx, b)
	spicedb := spice.NewClusterFromCockroachCluster(crdb, spice.WithDbName(dbName))
	tlog := e2e.NewTLog(b)
	require.NoError(b, spicedb.Start(ctx, tlog, ""))
	require.NoError(b, spicedb.Connect(ctx, tlog))

	// fill with tuples to ensure we span multiple ranges
	fill(b, spicedb[0].Client().V0().ACL(), "", 2000, 100)

	b.ResetTimer()

	checkNoNewEnemy(ctx, b, generateSchemaData(1, 1), 1, crdb, spicedb, b.N)
}

func fill(t testing.TB, client v0.ACLServiceClient, prefix string, fillerCount, batchSize int) {
	t.Log("filling prefix", prefix)
	require := require.New(t)
	directs, excludes := generateTuples(prefix, fillerCount, generator.NewUniqueGenerator(objIDRegex))
	for i := 0; i < fillerCount/batchSize; i++ {
		t.Log("filling", i*batchSize, "to", (i+1)*batchSize)
		_, err := client.Write(testCtx, &v0.WriteRequest{
			Updates: excludes[i*batchSize : (i+1)*batchSize],
		})
		require.NoError(err)
		_, err = client.Write(testCtx, &v0.WriteRequest{
			Updates: directs[i*batchSize : (i+1)*batchSize],
		})
		require.NoError(err)
	}
}

func fillSchema(t testing.TB, data []SchemaData, schemaClient v1alpha1.SchemaServiceClient) error {
	var b strings.Builder
	batchSize := len(data[0].Prefixes)
	for i, d := range data {
		t.Logf("filling %d to %d", i*batchSize, (i+1)*batchSize)
		b.Reset()
		err := schemaTpl.Execute(&b, d)
		if err != nil {
			return err
		}
		_, err = schemaClient.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
			Schema: b.String(),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// prefixForNode finds a prefix with namespace leaders on the node id
func prefixForNode(ctx context.Context, conn *pgx.Conn, data []SchemaData, node int) string {
	for {
		// get a random prefix
		d := data[rand.Intn(len(data))]
		candidate := d.Prefixes[rand.Intn(len(d.Prefixes))]
		ns1 := candidate + "/user"
		ns2 := candidate + "/resource"
		leader1 := getLeaderNodeForNamespace(ctx, conn, ns1)
		leader2 := getLeaderNodeForNamespace(ctx, conn, ns2)
		if leader1 == leader2 && leader1 == node {
			return candidate
		}
		select {
		case <-ctx.Done():
			return ""
		default:
			continue
		}
	}
}

func generateSchemaData(n int, batchSize int) (data []SchemaData) {
	data = make([]SchemaData, 0, n/batchSize)
	prefixGenerator := generator.NewUniqueGenerator(namespacePrefixRegex)
	for i := 0; i < n/batchSize; i++ {
		schema := SchemaData{Prefixes: make([]string, 0, batchSize)}
		for j := i * batchSize; j < (i+1)*batchSize; j++ {
			schema.Prefixes = append(schema.Prefixes, prefixGenerator.Next())
		}
		data = append(data, schema)
	}
	return
}

func generateTuples(prefix string, n int, objIdGenerator *generator.UniqueGenerator) (directs []*v0.RelationTupleUpdate, excludes []*v0.RelationTupleUpdate) {
	directs = make([]*v0.RelationTupleUpdate, 0, n)
	excludes = make([]*v0.RelationTupleUpdate, 0, n)
	for i := 0; i < n; i++ {
		user := &v0.User{
			UserOneof: &v0.User_Userset{
				Userset: &v0.ObjectAndRelation{
					Namespace: prefix + "/user",
					ObjectId:  objIdGenerator.Next(),
					Relation:  "...",
				},
			},
		}
		tupleExclude := &v0.RelationTuple{
			ObjectAndRelation: &v0.ObjectAndRelation{
				Namespace: prefix + "/resource",
				ObjectId:  "thegoods",
				Relation:  "excluded",
			},
			User: user,
		}
		tupleDirect := &v0.RelationTuple{
			ObjectAndRelation: &v0.ObjectAndRelation{
				Namespace: prefix + "/resource",
				ObjectId:  "thegoods",
				Relation:  "direct",
			},
			User: user,
		}
		excludes = append(excludes, &v0.RelationTupleUpdate{
			Operation: v0.RelationTupleUpdate_TOUCH,
			Tuple:     tupleExclude,
		})
		directs = append(directs, &v0.RelationTupleUpdate{
			Operation: v0.RelationTupleUpdate_TOUCH,
			Tuple:     tupleDirect,
		})
	}
	return
}

// getLeaderNode returns the node with the lease leader for the range containing the tuple
func getLeaderNode(ctx context.Context, conn *pgx.Conn, tuple *v0.RelationTuple) int {
	t := tuple
	rows, err := conn.Query(ctx, "SHOW RANGE FROM TABLE relation_tuple FOR ROW ($1::text,$2::text,$3::text,$4::text,$5::text,$6::text)",
		t.ObjectAndRelation.Namespace,
		t.ObjectAndRelation.ObjectId,
		t.ObjectAndRelation.Relation,
		t.User.GetUserset().Namespace,
		t.User.GetUserset().ObjectId,
		t.User.GetUserset().Relation,
	)
	defer rows.Close()
	if err != nil {
		log.Fatalf("failed to exec: %v", err)
	}
	return leaderFromRangeRow(rows)
}

// getLeaderNodeForNamespace returns the node with the lease leader for the range containing the namespace
func getLeaderNodeForNamespace(ctx context.Context, conn *pgx.Conn, namespace string) int {
	rows, err := conn.Query(ctx, "SHOW RANGE FROM TABLE namespace_config FOR ROW ($1::text)",
		namespace,
	)
	defer rows.Close()
	if err != nil {
		log.Fatalf("failed to exec: %v", err)
	}
	return leaderFromRangeRow(rows)
}

func leaderFromRangeRow(rows pgx.Rows) int {
	var (
		startKey           sql.NullString
		endKey             sql.NullString
		rangeID            int
		leaseHolder        int
		leaseHoldeLocality sql.NullString
		replicas           pgtype.Int8Array
		replicaLocalities  pgtype.TextArray
	)

	for rows.Next() {
		if err := rows.Scan(&startKey, &endKey, &rangeID, &leaseHolder, &leaseHoldeLocality, &replicas, &replicaLocalities); err != nil {
			panic(err)
		}
		break
	}
	return leaseHolder
}
