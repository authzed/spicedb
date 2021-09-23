package newenemy

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/e2e"
	"github.com/authzed/spicedb/pkg/zookie"
)

const schema = `
definition user {}
definition resource {
	relation direct: user
	relation excluded: user
	permission allowed = direct - excluded
}
`

const token = "testtesttesttest"

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
	setSmallRanges = "ALTER DATABASE %s CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 65536, num_replicas = 1;"
	dbName         = "spicedbnetest"
)

func startCluster(t testing.TB, ctx context.Context) CockroachCluster {
	require := require.New(t)

	fmt.Println("starting cockroach...")
	crdbCluster := NewCockroachCluster(3)
	for _, c := range crdbCluster {
		require.NoError(c.Start(ctx))
	}

	fmt.Println("initializing crdb...")
	crdbCluster.Init(ctx, os.Stdout, os.Stdout)
	require.NoError(crdbCluster.Sql(ctx, os.Stdout, os.Stdout,
		fmt.Sprintf(createDb, dbName),
	))

	fmt.Println("migrating...")
	require.NoError(MigrateHead(ctx, "cockroachdb", crdbCluster[0].ConnectionString(dbName)))

	fmt.Println("attempting to connect...")
	require.NoError(crdbCluster[2].Connect(ctx, os.Stdout, dbName))

	return crdbCluster
}

func TestNoNewEnemy(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	crdb := startCluster(t, ctx)

	t.Log("starting vulnerable spicedb...")
	vulnerableSpiceDb := NewSpiceClusterFromCockroachCluster(crdb, dbName, token)
	require.NoError(vulnerableSpiceDb.Start(ctx, os.Stdout, "vulnerable",
		"--datastore-tx-overlap-strategy=insecure"))
	require.NoError(vulnerableSpiceDb.Connect(ctx, os.Stdout))

	t.Log("configure small ranges, single replicas")
	require.NoError(crdb.Sql(ctx, os.Stdout, os.Stdout,
		fmt.Sprintf(setSmallRanges, dbName),
	))

	t.Log("modifying time")
	require.NoError(crdb.TimeDelay(ctx, e2e.MustFile(t, ctx, "timeattack.log"), 1, -200*time.Millisecond))

	t.Log("create initial test schema")
	require.NoError(initSchema(vulnerableSpiceDb[0].Client().V1Alpha1().Schema()))

	t.Log("filling with data to span multiple ranges")
	rand.Seed(time.Now().UnixNano())
	fill(vulnerableSpiceDb[0].Client().V0().ACL(), 4000, 100)

	t.Log("check vulnerability with mitigations disabled")
	checkCtx, checkCancel := context.WithTimeout(ctx, 2*time.Minute)
	protected, attempts := checkNoNewEnemy(t, checkCtx, vulnerableSpiceDb, crdb, -1)
	require.NotNil(protected, "unable to determine if spicedb displays newenemy when mitigations are disabled within the time limit")
	require.False(*protected)
	checkCancel()
	t.Logf("determined spicedb vulnerable in %d attempts", attempts)

	t.Log("stopping vulnerable spicedb cluster")
	require.NoError(vulnerableSpiceDb.Stop(os.Stdout))

	t.Log("start protected spicedb cluster")
	protectedSpiceDb := NewSpiceClusterFromCockroachCluster(crdb, dbName, token)
	require.NoError(protectedSpiceDb.Start(ctx, os.Stdout, "protected"))
	require.NoError(protectedSpiceDb.Connect(ctx, os.Stdout))

	t.Log("check spicedb is protected")
	checkCtx, checkCancel = context.WithTimeout(ctx, 2*time.Minute)
	protected, _ = checkNoNewEnemy(t, checkCtx, protectedSpiceDb, crdb, attempts)
	require.NotNil(protected, "unable to determine if spicedb is protected within the time limit")
	require.True(*protected, "protection is enabled, but newenemy detected")
	checkCancel()
}

// checkNoNewEnemy returns true if the service is protected, false if it is vulnerable, and nil if we couldn't determine
func checkNoNewEnemy(t testing.TB, ctx context.Context, spicedb SpiceCluster, crdb CockroachCluster, candidateCount int) (*bool, int) {
	var attempts, candidates int
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

		analyzeCalls(ctx, crdb[2].Conn(), os.Stdout, excludes[0].Tuple, directs[0].Tuple, r1.GetRevision(), r2.GetRevision(), &candidates)

		// let the timestamp caches get back out of sync
		time.Sleep(100 * time.Millisecond)

		if canHas.IsMember {
			t.Log("service is subject to the new enemy problem")
			protected := false
			return &protected, attempts
		}

		// if we find causal reversals, but no newenemy, assume we're protected
		if candidateCount > 0 && candidates >= candidateCount {
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
	crdb := startCluster(b, ctx)
	spicedb := NewSpiceClusterFromCockroachCluster(crdb, dbName, token)
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
	crdb := startCluster(b, ctx)
	spicedb := NewSpiceClusterFromCockroachCluster(crdb, dbName, token)
	require.NoError(b, spicedb.Start(ctx, os.Stdout, ""))
	require.NoError(b, spicedb.Connect(ctx, os.Stdout))

	// fill with tuples to ensure we span multiple ranges
	fill(spicedb[0].Client().V0().ACL(), 2000, 100)

	b.ResetTimer()

	// TODO
	// checkNoNewEnemy(b, ctx, spicedb, crdb, b.N*20, b.N)
}

func fill(client v0.ACLServiceClient, fillerCount, batchSize int) {
	directs, excludes := generateTuples(fillerCount)
	for i := 0; i < fillerCount/batchSize; i++ {
		fmt.Println("filling ", i*batchSize, "to", (i+1)*batchSize)
		_, err := client.Write(testCtx, &v0.WriteRequest{
			Updates: excludes[i*batchSize : (i+1)*batchSize],
		})
		if err != nil {
			fmt.Println(err)
		}
		_, err = client.Write(testCtx, &v0.WriteRequest{
			Updates: directs[i*batchSize : (i+1)*batchSize],
		})
		if err != nil {
			fmt.Println(err)
		}
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
func analyzeCalls(ctx context.Context, conn *pgx.Conn, out io.Writer, t1, t2 *v0.RelationTuple, r1, r2 *v0.Zookie, candidates *int) {
	l1, l1rs := getLeaderNode(ctx, conn, t1)
	l2, l2rs := getLeaderNode(ctx, conn, t2)

	z1, _ := zookie.DecodeRevision(r1)
	z2, _ := zookie.DecodeRevision(r2)

	// the best we can do when mitigations are enabled is guess that timestamps
	// with the same nanosecond timestamp were protected
	if z2.GreaterThan(z1) && z2.IntPart() == z1.IntPart() {
		*candidates++
		fmt.Fprintln(out, "candidate found")
	}

	fmt.Fprintln(out, z1, z2, z1.Sub(z2).String(), l1, l2, l1rs, l2rs)
}

// getLeaderNode returns the node with the lease leader for the range containing the tuple
func getLeaderNode(ctx context.Context, conn *pgx.Conn, tuple *v0.RelationTuple) (byte, []byte) {
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
	var raw []byte
	replicas := []byte{}
	for rows.Next() {
		// NOTE: this is in lieu of pulling in roachpb, it may be out of date
		// if those APIs change
		raw = rows.RawValues()[3]

		replicasRaw := rows.RawValues()[5]
		for {
			var found []byte
			replicasRaw, found = replicasRaw[:len(replicasRaw)-12], replicasRaw[len(replicasRaw)-12:]
			if found[3] == byte(8) {
				replicas = append(replicas, found[11])
			} else {
				break
			}
		}
		break
	}
	return raw[len(raw)-1], replicas
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
