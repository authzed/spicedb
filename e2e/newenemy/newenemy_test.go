package newenemy

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/spicedb/pkg/zookie"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/e2e"
	"github.com/authzed/spicedb/e2e/cockroach"
	"github.com/authzed/spicedb/e2e/spice"
)

const schema = `
definition user {}
definition resource {
	relation direct: user
	relation excluded: user
	permission allowed = direct - excluded
}
`

var testCtx context.Context

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

func startCluster(ctx context.Context, t testing.TB) cockroach.Cluster {
	require := require.New(t)

	fmt.Println("starting cockroach...")
	crdbCluster := cockroach.NewCluster(3)
	for _, c := range crdbCluster {
		require.NoError(c.Start(ctx))
	}

	fmt.Println("initializing crdb...")
	crdbCluster.Init(ctx, os.Stdout, os.Stdout)
	require.NoError(crdbCluster.SQL(ctx, os.Stdout, os.Stdout,
		fmt.Sprintf(createDb, dbName),
	))

	fmt.Println("migrating...")
	require.NoError(spice.MigrateHead(ctx, "cockroachdb", crdbCluster[0].ConnectionString(dbName)))

	fmt.Println("attempting to connect...")
	require.NoError(crdbCluster[2].Connect(ctx, os.Stdout, dbName))

	return crdbCluster
}

func TestNoNewEnemy(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	crdb := startCluster(ctx, t)

	t.Log("starting vulnerable spicedb...")
	vulnerableSpiceDb := spice.NewClusterFromCockroachCluster(crdb, spice.WithDbName(dbName))
	require.NoError(vulnerableSpiceDb.Start(ctx, os.Stdout, "vulnerable",
		"--datastore-tx-overlap-strategy=insecure"))
	require.NoError(vulnerableSpiceDb.Connect(ctx, os.Stdout))

	t.Log("start protected spicedb cluster")
	protectedSpiceDb := spice.NewClusterFromCockroachCluster(crdb,
		spice.WithGrpcPort(50061),
		spice.WithInternalPort(50062),
		spice.WithHttpPort(8444),
		spice.WithMetricsPort(9100),
		spice.WithDashboardPort(8100),
		spice.WithDbName(dbName))
	require.NoError(protectedSpiceDb.Start(ctx, os.Stdout, "protected"))
	require.NoError(protectedSpiceDb.Connect(ctx, os.Stdout))

	t.Log("configure small ranges, single replicas, short ttl")
	require.NoError(crdb.SQL(ctx, os.Stdout, os.Stdout,
		fmt.Sprintf(setSmallRanges, dbName),
	))

	t.Log("modifying time")
	timeDelay := 100 * time.Millisecond
	require.NoError(crdb.TimeDelay(ctx, e2e.MustFile(ctx, t, "timeattack.log"), 1, -1*timeDelay))

	t.Log("modifying network")
	networkDelay := timeDelay / 2
	require.NoError(crdb.NetworkDelay(ctx, e2e.MustFile(ctx, t, "netattack.log"), 1, networkDelay))

	t.Log("create initial test schema")
	require.NoError(initSchema(vulnerableSpiceDb[0].Client().V1Alpha1().Schema()))

	t.Log("filling with data to span multiple ranges")
	rand.Seed(time.Now().UnixNano())
	fill(require, vulnerableSpiceDb[0].Client().V0().ACL(), 4000, 100)

	const sampleSize = 5
	samples := make([]int, sampleSize)

	for i := 0; i < sampleSize; i++ {
		t.Log(i, "check vulnerability with mitigations disabled")
		checkCtx, checkCancel := context.WithTimeout(ctx, 5*time.Minute)
		protected, attempts := checkNoNewEnemy(checkCtx, t, vulnerableSpiceDb, -1)
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
	iterations := int(math.Ceil(3*stddev*samplestddev + mean))

	t.Logf("check spicedb is protected after %d attempts", iterations)
	// *6 to cover the worst case where all requests are handled by the slow node
	checkCtx, checkCancel := context.WithTimeout(ctx, time.Duration(iterations)*(networkDelay+timeDelay)*6)
	protected, _ := checkNoNewEnemy(checkCtx, t, protectedSpiceDb, iterations)
	require.NotNil(protected, "unable to determine if spicedb is protected within the time limit")
	require.True(*protected, "protection is enabled, but newenemy detected")
	checkCancel()
}

// checkNoNewEnemy returns true if the service is protected, false if it is vulnerable, and nil if we couldn't determine
func checkNoNewEnemy(ctx context.Context, t testing.TB, spicedb spice.Cluster, candidateCount int) (*bool, int) {
	var attempts int
	for {
		attempts++
		directs, excludes := generateTuples(1)

		// write to node 1
		r1, err := spicedb[0].Client().V0().ACL().Write(testCtx, &v0.WriteRequest{
			Updates: []*v0.RelationTupleUpdate{excludes[0]},
		})
		if err != nil {
			t.Log(err)
			continue
		}

		// write to node 2 (clock is behind)
		r2, err := spicedb[1].Client().V0().ACL().Write(testCtx, &v0.WriteRequest{
			Updates: []*v0.RelationTupleUpdate{directs[0]},
		})
		if err != nil {
			t.Log(err)
			continue
		}

		canHas, err := spicedb[2].Client().V0().ACL().Check(context.Background(), &v0.CheckRequest{
			TestUserset: &v0.ObjectAndRelation{
				Namespace: "resource",
				ObjectId:  "thegoods",
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

		analyzeCalls(os.Stdout, r1.GetRevision(), r2.GetRevision())

		if canHas.IsMember {
			t.Log("service is subject to the new enemy problem")
			protected := false
			return &protected, attempts
		}

		// if we find causal reversals, but no newenemy, assume we're protected
		if candidateCount > 0 && attempts >= candidateCount {
			t.Log(candidateCount, "(would be) causal reversals with no new enemy detected")
			protected := true
			return &protected, attempts
		}

		select {
		case <-ctx.Done():
			return nil, attempts
		default:
			continue
		}
	}
}

func BenchmarkBatchWrites(b *testing.B) {
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	crdb := startCluster(ctx, b)
	spicedb := spice.NewClusterFromCockroachCluster(crdb, spice.WithDbName(dbName))
	require.NoError(b, spicedb.Start(ctx, os.Stdout, ""))
	require.NoError(b, spicedb.Connect(ctx, os.Stdout))

	exludes, directs := generateTuples(b.N * 20)
	b.ResetTimer()
	_, err := spicedb[0].Client().V0().ACL().Write(testCtx, &v0.WriteRequest{
		Updates: exludes,
	})
	if err != nil {
		fmt.Println(err)
	}
	_, err = spicedb[0].Client().V0().ACL().Write(testCtx, &v0.WriteRequest{
		Updates: directs,
	})
	if err != nil {
		fmt.Println(err)
	}
}

func BenchmarkConflictingTupleWrites(b *testing.B) {
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	crdb := startCluster(ctx, b)
	spicedb := spice.NewClusterFromCockroachCluster(crdb, spice.WithDbName(dbName))
	require.NoError(b, spicedb.Start(ctx, os.Stdout, ""))
	require.NoError(b, spicedb.Connect(ctx, os.Stdout))

	// fill with tuples to ensure we span multiple ranges
	fill(require.New(b), spicedb[0].Client().V0().ACL(), 2000, 100)

	b.ResetTimer()

	checkNoNewEnemy(ctx, b, spicedb, b.N)
}

func fill(require *require.Assertions, client v0.ACLServiceClient, fillerCount, batchSize int) {
	directs, excludes := generateTuples(fillerCount)
	for i := 0; i < fillerCount/batchSize; i++ {
		fmt.Println("filling ", i*batchSize, "to", (i+1)*batchSize)
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

func initSchema(schemaClient v1alpha1.SchemaServiceClient) error {
	_, err := schemaClient.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: schema,
	})
	return err
}

func generateTuples(n int) (directs []*v0.RelationTupleUpdate, excludes []*v0.RelationTupleUpdate) {
	directs = make([]*v0.RelationTupleUpdate, 0, n)
	excludes = make([]*v0.RelationTupleUpdate, 0, n)
	for i := 0; i < n; i++ {
		user := &v0.User{
			UserOneof: &v0.User_Userset{
				Userset: &v0.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  randSeq(16),
					Relation:  "...",
				},
			},
		}
		tupleExclude := &v0.RelationTuple{
			ObjectAndRelation: &v0.ObjectAndRelation{
				Namespace: "resource",
				ObjectId:  "thegoods",
				Relation:  "excluded",
			},
			User: user,
		}
		tupleDirect := &v0.RelationTuple{
			ObjectAndRelation: &v0.ObjectAndRelation{
				Namespace: "resource",
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

// after we've checked, analyze the previous calls
func analyzeCalls(out io.Writer, r1, r2 *v0.Zookie) {
	z1, _ := zookie.DecodeRevision(r1)
	z2, _ := zookie.DecodeRevision(r2)

	// the best we can do when mitigations are enabled is guess that timestamps
	// with the same nanosecond timestamp were protected
	if z2.GreaterThan(z1) && z2.IntPart() == z1.IntPart() {
		fmt.Fprintln(out, "candidate found")
	}

	fmt.Fprintln(out, z1, z2, z1.Sub(z2).String())
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
