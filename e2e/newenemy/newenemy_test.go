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

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/e2e"
	"github.com/authzed/spicedb/e2e/cockroach"
	"github.com/authzed/spicedb/e2e/generator"
	"github.com/authzed/spicedb/e2e/spice"
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

const schemaText = `
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

const schemaOriginalText = `
{{ range .Namespaces }}
definition {{.}}/user {}
definition {{.}}/resource {
	relation direct: {{.}}/user
	relation excluded: {{.}}/user
	permission allowed = direct - excluded
}
{{ end }}
`

const schemaAllowAllText = `
{{ range .Namespaces }}
definition {{.}}/user {}
definition {{.}}/resource {
	relation direct: {{.}}/user
	relation excluded: {{.}}/user
	permission allowed = direct
}
{{ end }}
`

const (
	objIDRegex           = "[a-zA-Z0-9_][a-zA-Z0-9/_-]{0,127}"
	namespacePrefixRegex = "[a-z][a-z0-9_]{1,62}[a-z0-9]"
)

var (
	maxIterations = flag.Int("max-iterations", 1000, "iteration cap for statistic-based tests (0 for no limit)")

	schemaTpl         = template.Must(template.New("schema").Parse(schemaText))
	schemaOriginalTpl = template.Must(template.New("schema_orig").Parse(schemaOriginalText))
	schemaAllowTpl    = template.Must(template.New("schema_allow").Parse(schemaAllowAllText))
	objIdGenerator    = generator.NewUniqueGenerator(objIDRegex)
	prefixGenerator   = generator.NewUniqueGenerator(namespacePrefixRegex)

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
	createDb       = "CREATE DATABASE %s;"
	setSmallRanges = "ALTER DATABASE %s CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 65536, num_replicas = 1, gc.ttlseconds = 10;"
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
		"SET CLUSTER SETTING kv.range.backpressure_range_size_multiplier=0;",
	))
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
	rand.Seed(time.Now().UnixNano())
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	crdb := initializeTestCRDBCluster(ctx, t)

	tlog := e2e.NewTLog(t)

	t.Log("starting vulnerable spicedb...")
	vulnerableSpiceDb := spice.NewClusterFromCockroachCluster(crdb, spice.WithDbName(dbName))
	require.NoError(t, vulnerableSpiceDb.Start(ctx, tlog, "vulnerable",
		"--datastore-tx-overlap-strategy=insecure"))
	require.NoError(t, vulnerableSpiceDb.Connect(ctx, tlog))
	t.Cleanup(func() {
		require.NoError(t, vulnerableSpiceDb.Stop(tlog))
	})

	t.Log("start protected spicedb cluster")
	protectedSpiceDb := spice.NewClusterFromCockroachCluster(crdb,
		spice.WithGrpcPort(50061),
		spice.WithDispatchPort(50062),
		spice.WithHttpPort(8444),
		spice.WithMetricsPort(9100),
		spice.WithDashboardPort(8100),
		spice.WithDbName(dbName))
	require.NoError(t, protectedSpiceDb.Start(ctx, tlog, "protected"))
	require.NoError(t, protectedSpiceDb.Connect(ctx, tlog))
	t.Cleanup(func() {
		require.NoError(t, protectedSpiceDb.Stop(tlog))
	})

	t.Log("configure small ranges, single replicas, short ttl")
	require.NoError(t, crdb.SQL(ctx, tlog, tlog,
		fmt.Sprintf(setSmallRanges, dbName),
	))

	t.Log("fill with schemas to span multiple ranges")
	// 4000 is larger than we need to span all three nodes, but a higher number
	// seems to make the test converge faster
	schemaData := generateSchemaData(4000, 500)
	fillSchema(t, schemaTpl, schemaData, vulnerableSpiceDb[1].Client().V1Alpha1().Schema())
	slowNodeId, err := crdb[1].NodeID(ctx)
	require.NoError(t, err)

	t.Log("modifying time")
	require.NoError(t, crdb.TimeDelay(ctx, e2e.MustFile(ctx, t, "timeattack-1.log"), 1, -200*time.Millisecond))

	tests := []struct {
		name            string
		vulnerableProbe probeFn
		protectedProbe  probeFn
		vulnerableMax   int
		sampleSize      int
	}{
		// {
		// 	name: "protected from schema newenemy",
		// 	vulnerableProbe: func(count int) (bool, int) {
		// 		return checkSchemaNoNewEnemy(ctx, t, schemaData, slowNodeId, crdb, vulnerableSpiceDb, count)
		// 	},
		// 	protectedProbe: func(count int) (bool, int) {
		// 		return checkSchemaNoNewEnemy(ctx, t, schemaData, slowNodeId, crdb, protectedSpiceDb, count)
		// 	},
		// 	vulnerableMax: 100,
		// 	sampleSize:    5,
		// },
		// {
		// 	name: "protected from data newenemy",
		// 	vulnerableProbe: func(count int) (bool, int) {
		// 		return checkDataNoNewEnemy(ctx, t, schemaData, slowNodeId, crdb, vulnerableSpiceDb, count)
		// 	},
		// 	protectedProbe: func(count int) (bool, int) {
		// 		return checkDataNoNewEnemy(ctx, t, schemaData, slowNodeId, crdb, protectedSpiceDb, count)
		// 	},
		// 	vulnerableMax: 100,
		// 	sampleSize:    5,
		// },
		{
			name: "protected from data newenemy 2",
			vulnerableProbe: func(count int) (bool, int) {
				return checkDataNoNewEnemyNew(ctx, t, schemaData, slowNodeId, crdb, vulnerableSpiceDb, count, true)
			},
			protectedProbe: func(count int) (bool, int) {
				return checkDataNoNewEnemyNew(ctx, t, schemaData, slowNodeId, crdb, protectedSpiceDb, count, false)
			},
			vulnerableMax: 20,
			sampleSize:    5,
		},
	}
	for _, tt := range tests {
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
	return
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
	return
}

func checkDataNoNewEnemyNew(ctx context.Context, t testing.TB, schemaData []SchemaData, slowNodeId int, crdb cockroach.Cluster, spicedb spice.Cluster, maxAttempts int, exitEarly bool) (bool, int) {
	prefix := prefixesForNode(t, ctx, crdb[1].Conn(), spicedb[0].Client().V1Alpha1().Schema(), slowNodeId)
	t.Log("filling with data to span multiple ranges for prefix", prefix)
	fill(ctx, t, spicedb[0].Client().V1().Permissions(), prefix, 1000, 500)
	allowlists, blocklists, allowusers, blockusers := generateTuples(prefix, maxAttempts, objIdGenerator)

	for i := 0; i < maxAttempts; i++ {
		// write prereqs
		_, err := spicedb[0].Client().V1().Permissions().WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: []*v1.RelationshipUpdate{allowusers[i], blocklists[i]},
		})
		require.NoError(t, err)
	}

	time.Sleep(500 * time.Millisecond)

	for i := 0; i < maxAttempts; i++ {
		// exclude user by connecting user to blocklist
		r1, err := spicedb[0].Client().V1().Permissions().WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: []*v1.RelationshipUpdate{blockusers[i]},
		})
		require.NoError(t, err)

		// the first write has to read the namespaces from the second node,
		// which will resync the timestamps. sleeping allows the clocks to get
		// back out of sync
		sleep := (time.Duration(i) * 10) % 100 * time.Millisecond
		time.Sleep(sleep)
		// time.Sleep(50 * time.Millisecond)

		// write to node 2 (clock is behind)
		// allow user by adding allowlist to resource
		r2, err := spicedb[1].Client().V1().Permissions().WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: []*v1.RelationshipUpdate{allowlists[i]},
		})
		require.NoError(t, err)

		// time.Sleep(200 * time.Millisecond)

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

		ns1BlocklistLeader := getLeaderNodeForNamespace(ctx, crdb[2].Conn(), blockusers[i].Relationship.Resource.ObjectType)
		ns1UserLeader := getLeaderNodeForNamespace(ctx, crdb[2].Conn(), blockusers[i].Relationship.Subject.Object.ObjectType)
		ns2ResourceLeader := getLeaderNodeForNamespace(ctx, crdb[2].Conn(), allowlists[i].Relationship.Resource.ObjectType)
		ns2AllowlistLeader := getLeaderNodeForNamespace(ctx, crdb[2].Conn(), allowlists[i].Relationship.Subject.Object.ObjectType)

		r1leader, r2leader := getLeaderNode(ctx, crdb[2].Conn(), blockusers[i].Relationship), getLeaderNode(ctx, crdb[2].Conn(), allowlists[i].Relationship)
		z1, _ := zedtoken.DecodeRevision(r1.WrittenAt)
		z2, _ := zedtoken.DecodeRevision(r2.WrittenAt)
		t.Log(sleep, z1, z2, z1.Sub(z2).String(), r1leader, r2leader, ns1BlocklistLeader, ns1UserLeader, ns2ResourceLeader, ns2AllowlistLeader)

		if z1.Sub(z2).IsPositive() {
			t.Log("timestamps are reversed", canHas.Permissionship.String())
		}

		if canHas.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
			t.Log("service is subject to the new enemy problem")
			return false, i + 1
		}

		if ns2ResourceLeader != slowNodeId || ns2AllowlistLeader != slowNodeId {
			t.Log("namespace leader changed, pick new namespaces and fill")
			// returning true will re-run with a new prefix
			return true, i + 1
		}

		// if r1leader == r2leader && i%2 == 0 {
		// 	t.Log("both writes to same node, pick new namespace")
		// }
		//
		if r2leader == ns1UserLeader && i%5 == 4 && exitEarly {
			t.Log("second write to fast node, pick new namespaces")
			return true, i + 1
		}

		if r1leader == ns2ResourceLeader && i%5 == 4 && exitEarly {
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

// checkDataNoNewEnemy returns true if the service is protected and false if it
// is vulnerable.
//
// This subtest ensures protection from a "data-based" newenemy problem:
//
// 1. Use this schema:
//      definition user {}
//
//      definition blocklist {
//      	relation user: user
//      }
//
//      definition resource {
//        relation excluded: blocklist
// 	      relation direct: user
// 	      permission allowed = direct - excluded->user
//      }
// 2. Write resource:1#excluded@user:A
// 3. Write resource:2#direct:@user:A
// 4. If the timestamp from (3) is before the timestamp for (2), then:
//       check(resource:1#allowed@user:A)
//    may succeed, when it should fail. The timestamps can be reversed
//    if the tx overlap protections are disabled, because cockroach only
//    ensures overlapping transactions are linearizable.
// func checkDataNoNewEnemy(ctx context.Context, t testing.TB, schemaData []SchemaData, slowNodeId int, crdb cockroach.Cluster, spicedb spice.Cluster, maxAttempts int) (bool, int) {
// 	prefix := prefixesForNode(ctx, crdb[1].Conn(), schemaData, slowNodeId)
// 	t.Log("filling with data to span multiple ranges for prefix", prefix)
// 	fill(ctx, t, spicedb[0].Client().V1().Permissions(), prefix, 500, 100)
//
// 	time.Sleep(500 * time.Millisecond)
//
// 	var sleep time.Duration = 50 * time.Millisecond
// 	for attempts := 1; attempts <= maxAttempts; attempts++ {
// 		// sleep += 10
// 		direct, exclude := generateOriginalTuple(prefix, objIdGenerator)
//
// 		// // write prereqs
// 		// _, err := spicedb[0].Client().V1().Permissions().WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
// 		// 	Updates: []*v1.RelationshipUpdate{allowuser, blocklist},
// 		// })
// 		// require.NoError(t, err)
// 		//
// 		// time.Sleep(100 * time.Millisecond)
//
// 		// exclude user by connecting user to blocklist
// 		r1, err := spicedb[0].Client().V1().Permissions().WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
// 			Updates: []*v1.RelationshipUpdate{exclude},
// 		})
// 		require.NoError(t, err)
//
// 		// the first write has to read the namespaces from the second node,
// 		// which will resync the timestamps. sleeping allows the clocks to get
// 		// back out of sync
// 		time.Sleep(sleep * time.Millisecond)
//
// 		// write to node 2 (clock is behind)
// 		// allow user by adding allowlist to resource
// 		r2, err := spicedb[1].Client().V1().Permissions().WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
// 			Updates: []*v1.RelationshipUpdate{direct},
// 		})
// 		require.NoError(t, err)
//
// 		canHas, err := spicedb[1].Client().V1().Permissions().CheckPermission(context.Background(), &v1.CheckPermissionRequest{
// 			Consistency: &v1.Consistency{Requirement: &v1.Consistency_AtExactSnapshot{AtExactSnapshot: r2.WrittenAt}},
// 			Resource: &v1.ObjectReference{
// 				ObjectType: direct.Relationship.Resource.ObjectType,
// 				ObjectId:   direct.Relationship.Resource.ObjectId,
// 			},
// 			Permission: "allowed",
// 			Subject: &v1.SubjectReference{
// 				Object: &v1.ObjectReference{
// 					ObjectType: direct.Relationship.Subject.Object.ObjectType,
// 					ObjectId:   direct.Relationship.Subject.Object.ObjectId,
// 				},
// 			},
// 		})
// 		require.NoError(t, err)
//
// 		r1leader, r2leader := getLeaderNode(ctx, crdb[1].Conn(), direct.Relationship), getLeaderNode(ctx, crdb[1].Conn(), exclude.Relationship)
// 		ns1Leader := getLeaderNodeForNamespace(ctx, crdb[1].Conn(), direct.Relationship.Resource.ObjectType)
// 		ns2Leader := getLeaderNodeForNamespace(ctx, crdb[1].Conn(), direct.Relationship.Subject.Object.ObjectType)
// 		z1, _ := zedtoken.DecodeRevision(r1.WrittenAt)
// 		z2, _ := zedtoken.DecodeRevision(r2.WrittenAt)
// 		t.Log(z1, z2, z1.Sub(z2).String(), r1leader, r2leader, ns1Leader, ns2Leader)
//
// 		if canHas.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
// 			t.Log("service is subject to the new enemy problem")
// 			return false, attempts
// 		}
//
// 		if ns1Leader != slowNodeId || ns2Leader != slowNodeId {
// 			t.Log("namespace leader changed, pick new prefix and fill")
// 			// returning true will re-run with a new prefix
// 			return true, attempts
// 		}
//
// 		if z1.Sub(z2).IsPositive() {
// 			t.Log("doesn't have permission, but timestamps are reversed")
// 			continue
// 		}
//
// 		select {
// 		case <-ctx.Done():
// 			return false, attempts
// 		default:
// 			// this sleep helps the clocks get back out of sync after an attempt
// 			time.Sleep(300 * time.Millisecond)
// 			continue
// 		}
// 	}
// 	return true, maxAttempts
// }

// checkSchemaNoNewEnemy returns true if the service is protected, false if it
// is vulnerable.
//
// This test ensures protection from a "schema and data" newenemy, i.e.
// the new enemy conditions require linearizable changes to schema and data
// 1. Start with this schema:
//      definition user {}
//      definition resource {
//	      relation direct: user
//	      relation excluded: user
//	      permission allowed = direct
//      }
// 2. Write resource:1#direct:@user:A
// 3. Write resource:1#excluded@user:A
// 4. Update to this schema:
//      definition user {}
//      definition resource {
//	      relation direct: user
//	      relation excluded: user
//	      permission allowed = direct - excluded
//      }
// 5. If the revision from (4) is before the timestamp for (3), then:
//       check(revision from 3, resource:1#allowed@user:A)
//    may fail, when it should succeed. The timestamps can be reversed
//    if the tx overlap protections are disabled, because cockroach only
//    ensures overlapping transactions are linearizable.
//    In this case, we don't get an explicit revision back from the
//    WriteSchema call, but the Schema write and the resource write are
//    fully consistent.
// func checkSchemaNoNewEnemy(ctx context.Context, t testing.TB, schemaData []SchemaData, slowNodeId int, crdb cockroach.Cluster, spicedb spice.Cluster, maxAttempts int) (bool, int) {
// 	prefix := prefixForNode(ctx, crdb[1].Conn(), schemaData, slowNodeId)
// 	var b strings.Builder
// 	require.NoError(t, schemaAllowTpl.Execute(&b, SchemaData{NamespaceNames: []string{prefix}}))
// 	allowSchema := b.String()
// 	b.Reset()
// 	require.NoError(t, schemaTpl.Execute(&b, SchemaData{NamespaceNames: []string{prefix}}))
// 	excludeSchema := b.String()
//
// 	for attempts := 1; attempts <= maxAttempts; attempts++ {
// 		direct, exclude := generateTuple(prefix, objIdGenerator)
//
// 		// write the "allow" schema
// 		require.NoError(t, getErr(spicedb[0].Client().V1Alpha1().Schema().WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
// 			Schema: allowSchema,
// 		})))
//
// 		// write the "direct" tuple
// 		require.NoError(t, getErr(spicedb[0].Client().V0().ACL().Write(ctx, &v0.WriteRequest{
// 			Updates: []*v0.RelationTupleUpdate{direct},
// 		})))
//
// 		// write the "excludes" tuple
// 		// writing to 1 primes the namespace cache on node 1 with the "allow" namespace
// 		r2, err := spicedb[1].Client().V0().ACL().Write(ctx, &v0.WriteRequest{
// 			Updates: []*v0.RelationTupleUpdate{exclude},
// 		})
// 		require.NoError(t, err)
//
// 		// write the "exclude" schema. If this write hits the slow crdb node, it
// 		// can get a revision in between the direct and exclude tuple writes
// 		// which will cause check to fail, when it should succeed
// 		require.NoError(t, getErr(spicedb[1].Client().V1Alpha1().Schema().WriteSchema(ctx, &v1alpha1.WriteSchemaRequest{
// 			Schema: excludeSchema,
// 		})))
//
// 		rev, err := zookie.DecodeRevision(core.ToCoreZookie(r2.GetRevision()))
// 		require.NoError(t, err)
//
// 		var canHas *v1.CheckPermissionResponse
// 		checkAccess := func() bool {
// 			var err error
//
// 			canHas, err = spicedb[0].Client().V1().Permissions().CheckPermission(ctx, &v1.CheckPermissionRequest{
// 				Consistency: &v1.Consistency{
// 					Requirement: &v1.Consistency_AtExactSnapshot{AtExactSnapshot: zedtoken.NewFromRevision(rev)},
// 				},
// 				Resource: &v1.ObjectReference{
// 					ObjectType: direct.Tuple.ObjectAndRelation.Namespace,
// 					ObjectId:   direct.Tuple.ObjectAndRelation.ObjectId,
// 				},
// 				Permission: "allowed",
// 				Subject: &v1.SubjectReference{
// 					Object: &v1.ObjectReference{
// 						ObjectType: direct.Tuple.User.GetUserset().Namespace,
// 						ObjectId:   direct.Tuple.User.GetUserset().ObjectId,
// 					},
// 				},
// 			})
// 			if err != nil {
// 				t.Log(err)
// 			}
// 			return err == nil
// 		}
// 		require.Eventually(t, checkAccess, 10*time.Second, 10*time.Millisecond)
//
// 		if canHas.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION {
// 			t.Log("service is subject to the new enemy problem")
// 			return false, attempts
// 		}
//
// 		select {
// 		case <-ctx.Done():
// 			return false, attempts
// 		default:
// 			// this is not strictly needed, but helps avoid write contention
// 			time.Sleep(100 * time.Millisecond)
// 			continue
// 		}
// 	}
// 	return true, maxAttempts
// }

// func BenchmarkBatchWrites(b *testing.B) {
// 	ctx, cancel := context.WithCancel(testCtx)
// 	defer cancel()
// 	crdb := initializeTestCRDBCluster(ctx, b)
// 	spicedb := spice.NewClusterFromCockroachCluster(crdb, spice.WithDbName(dbName))
// 	tlog := e2e.NewTLog(b)
// 	require.NoError(b, spicedb.Start(ctx, tlog, ""))
// 	require.NoError(b, spicedb.Connect(ctx, tlog))
//
// 	directs, excludes := generateTuples("", b.N*20, objIdGenerator)
// 	b.ResetTimer()
// 	_, err := spicedb[0].Client().V0().ACL().Write(ctx, &v0.WriteRequest{
// 		Updates: excludes,
// 	})
// 	if err != nil {
// 		b.Log(err)
// 	}
// 	_, err = spicedb[0].Client().V0().ACL().Write(ctx, &v0.WriteRequest{
// 		Updates: directs,
// 	})
// 	if err != nil {
// 		b.Log(err)
// 	}
// }

func BenchmarkConflictingTupleWrites(b *testing.B) {
	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	crdb := initializeTestCRDBCluster(ctx, b)
	spicedb := spice.NewClusterFromCockroachCluster(crdb, spice.WithDbName(dbName))
	tlog := e2e.NewTLog(b)
	require.NoError(b, spicedb.Start(ctx, tlog, ""))
	require.NoError(b, spicedb.Connect(ctx, tlog))

	// fill with tuples to ensure we span multiple ranges
	fill(ctx, b, spicedb[0].Client().V1().Permissions(), NamespaceNames{}, 2000, 100)

	b.ResetTimer()

	// checkDataNoNewEnemy(ctx, b, generateSchemaData(1, 1), 1, crdb, spicedb, b.N)
}

func fill(ctx context.Context, t testing.TB, client v1.PermissionsServiceClient, prefix NamespaceNames, fillerCount, batchSize int) {
	t.Log("filling prefix", prefix)
	require := require.New(t)
	allowlists, blocklists, allowusers, blockusers := generateTuples(prefix, fillerCount, objIdGenerator)
	for i := 0; i < fillerCount/batchSize; i++ {
		t.Log("filling", i*batchSize, "to", (i+1)*batchSize)
		_, err := client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: allowlists[i*batchSize : (i+1)*batchSize],
		})
		require.NoError(err)
		_, err = client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: blocklists[i*batchSize : (i+1)*batchSize],
		})
		require.NoError(err)
		_, err = client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: allowusers[i*batchSize : (i+1)*batchSize],
		})
		require.NoError(err)
		_, err = client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: blockusers[i*batchSize : (i+1)*batchSize],
		})
		require.NoError(err)
	}
}

func fillallow(ctx context.Context, t testing.TB, client v1.PermissionsServiceClient, prefix NamespaceNames, fillerCount, batchSize int) {
	t.Log("filling prefix", prefix)
	require := require.New(t)
	allowlists, _, _, _ := generateTuples(prefix, fillerCount, objIdGenerator)
	for i := 0; i < fillerCount/batchSize; i++ {
		t.Log("filling", i*batchSize, "to", (i+1)*batchSize)
		_, err := client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: allowlists[i*batchSize : (i+1)*batchSize],
		})
		require.NoError(err)
	}
}

// fillSchema generates the schema text for given SchemaData and applies it
func fillSchema(t testing.TB, template *template.Template, data []SchemaData, schemaClient v1alpha1.SchemaServiceClient) {
	var b strings.Builder
	batchSize := len(data[0].Namespaces)
	for i, d := range data {
		t.Logf("filling %d to %d", i*batchSize, (i+1)*batchSize)
		b.Reset()
		require.NoError(t, template.Execute(&b, d))
		_, err := schemaClient.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
			Schema: b.String(),
		})
		require.NoError(t, err)
	}
}

// prefixesForNode finds a prefix with namespace leaders on the node id
func prefixesForNode(t testing.TB, ctx context.Context, conn *pgx.Conn, schemaClient v1alpha1.SchemaServiceClient, node int) NamespaceNames {
	// blocklist and user need to be on !node
	// allowlist and resource need to be on node

	for {
		newSchema := generateSchemaData(1, 1)
		p := newSchema[0].Namespaces[0]
		var b strings.Builder
		require.NoError(t, schemaTpl.Execute(&b, newSchema[0]))
		_, err := schemaClient.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
			Schema: b.String(),
		})
		require.NoError(t, err)

		userLeader := getLeaderNodeForNamespace(ctx, conn, p.User)
		resourceLeader := getLeaderNodeForNamespace(ctx, conn, p.Resource)
		allowlistLeader := getLeaderNodeForNamespace(ctx, conn, p.Allowlist)
		blocklistLeader := getLeaderNodeForNamespace(ctx, conn, p.Blocklist)

		// allowlist and resource must be equal to node
		// blocklist and user must not be equal to node
		if blocklistLeader == userLeader && userLeader != node && allowlistLeader == resourceLeader && resourceLeader == node {
			fmt.Println("found namespaces", p.User, userLeader, p.Resource, resourceLeader, p.Allowlist, allowlistLeader, p.Blocklist, blocklistLeader)
			return p
		}

		select {
		case <-ctx.Done():
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
	return
}

func generateTuple(names NamespaceNames, objIdGenerator *generator.UniqueGenerator) (allowlist *v1.RelationshipUpdate, blocklist *v1.RelationshipUpdate, allowuser *v1.RelationshipUpdate, blockuser *v1.RelationshipUpdate) {
	a, b, c, d := generateTuples(names, 1, objIdGenerator)
	return a[0], b[0], c[0], d[0]
}

func generateTuples(names NamespaceNames, n int, objIdGenerator *generator.UniqueGenerator) (allowlists []*v1.RelationshipUpdate, blocklists []*v1.RelationshipUpdate, allowusers []*v1.RelationshipUpdate, blockusers []*v1.RelationshipUpdate) {
	allowlists = make([]*v1.RelationshipUpdate, 0, n)
	blocklists = make([]*v1.RelationshipUpdate, 0, n)
	allowusers = make([]*v1.RelationshipUpdate, 0, n)
	blockusers = make([]*v1.RelationshipUpdate, 0, n)

	for i := 0; i < n; i++ {
		user := &v1.ObjectReference{
			ObjectType: names.User,
			ObjectId:   objIdGenerator.Next(),
		}

		allowlistID := objIdGenerator.Next()
		blocklistID := objIdGenerator.Next()

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
	return
}

func generateOriginalTuple(prefix NamespaceNames, objIdGenerator *generator.UniqueGenerator) (directs *v1.RelationshipUpdate, excludes *v1.RelationshipUpdate) {
	a, b := generateOriginalTuples(prefix, 1, objIdGenerator)
	return a[0], b[0]
}

func generateOriginalTuples(prefix NamespaceNames, n int, objIdGenerator *generator.UniqueGenerator) (directs []*v1.RelationshipUpdate, excludes []*v1.RelationshipUpdate) {
	directs = make([]*v1.RelationshipUpdate, 0, n)
	excludes = make([]*v1.RelationshipUpdate, 0, n)

	for i := 0; i < n; i++ {
		user := &v1.ObjectReference{
			ObjectType: prefix.User + "/user",
			ObjectId:   objIdGenerator.Next(),
		}

		resourceID := objIdGenerator.Next()

		tupleDirect := &v1.Relationship{
			Resource: &v1.ObjectReference{
				ObjectType: prefix.Resource + "/resource",
				ObjectId:   resourceID,
			},
			Relation: "direct",
			Subject:  &v1.SubjectReference{Object: user},
		}

		tupleExcludes := &v1.Relationship{
			Resource: &v1.ObjectReference{
				ObjectType: prefix.Resource + "/resource",
				ObjectId:   resourceID,
			},
			Relation: "excluded",
			Subject:  &v1.SubjectReference{Object: user},
		}

		directs = append(directs, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: tupleDirect,
		})
		excludes = append(excludes, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: tupleExcludes,
		})
	}
	return
}

// getLeaderNode returns the node with the lease leader for the range containing the tuple
func getLeaderNode(ctx context.Context, conn *pgx.Conn, tuple *v1.Relationship) int {
	t := tuple
	rows, err := conn.Query(ctx, "SHOW RANGE FROM TABLE relation_tuple FOR ROW ($1::text,$2::text,$3::text,$4::text,$5::text,$6::text)",
		t.Resource.ObjectType,
		t.Resource.ObjectId,
		t.Relation,
		t.Subject.Object.ObjectType,
		t.Subject.Object.ObjectId,
		t.Subject.OptionalRelation,
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

// leaderFromRangeRow parses the rows from a `SHOW RANGE` query and returns the
// leader node id for the range
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

func getErr(vals ...interface{}) error {
	if len(vals) == 0 {
		return nil
	}
	err := vals[len(vals)-1]
	if err == nil {
		return nil
	}
	if err, ok := err.(error); ok {
		return err
	}
	return nil
}
