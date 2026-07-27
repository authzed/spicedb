//go:build image

package cache_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/testutil/sdbtestcontainer"
)

const (
	pgNetworkAlias      = "db"
	spicedbNetworkAlias = "spicedb"

	// spicedbFanout is the number of union arms on document.view. Each check walks
	// every arm (only the last is satisfied, so the union does not
	// short-circuit) and each arm dispatches to a distinct group, so one check
	// populates several dispatch-cache entries. This fills the dispatch and
	// dispatch-cluster caches quickly under Check load.
	spicedbFanout = 8
)

// TestCacheFillingDoesNotTripMemoryLimiter drives the dispatch caches to their
// configured budgets under Check load and asserts the memory-protection
// middleware never sheds a valid request.
func TestCacheFillingDoesNotTripMemoryLimiter(t *testing.T) {
	image := envOr("SPICEDB_IMAGE", "authzed/spicedb:ci")

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()

	net, err := network.New(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = net.Remove(t.Context()) })

	pg := testdatastore.RunPostgresForTesting(t, "16", false,
		network.WithNetwork([]string{pgNetworkAlias}, net))
	var migratedHostURI string
	pg.NewDatastore(t, func(_, uri string) datastore.Datastore {
		migratedHostURI = uri
		return nil
	})
	connURI := inNetworkURI(t, migratedHostURI)

	spicedb, err := sdbtestcontainer.Run(ctx, image,
		sdbtestcontainer.WithDatastore("postgres", connURI),
		network.WithNetwork([]string{spicedbNetworkAlias}, net),
		testcontainers.WithEnv(map[string]string{
			// GOMEMLIMIT bounds the server heap; the memory-protection middleware
			// rejects requests with RESOURCE_EXHAUSTED as the heap approaches it. The
			// value is chosen so a server that evicts its caches at their configured
			// budgets stays comfortably below it under this load.
			"GOMEMLIMIT": envOr("SPICEDB_GOMEMLIMIT", "450MiB"),
			// Collect frequently so transient per-request garbage is reclaimed and the
			// live heap (dominated by the dispatch caches) is what the memory limiter sees.
			"GOGC": envOr("SPICEDB_GOGC", "25"),
			// A long quantization window with staleness gives dispatch cache
			// entries a sliding TTL of roughly 2 x (5m x 1.1) = 11m, so nothing
			// expires during the run and the caches reach their budgets.
			"SPICEDB_DATASTORE_REVISION_QUANTIZATION_INTERVAL":              "5m",
			"SPICEDB_DATASTORE_REVISION_QUANTIZATION_MAX_STALENESS_PERCENT": "10",
			// Small absolute budgets, identical across runs regardless of host.
			"SPICEDB_DISPATCH_CACHE_MAX_COST":         "24MiB",
			"SPICEDB_DISPATCH_CLUSTER_CACHE_MAX_COST": "24MiB",
			"SPICEDB_NS_CACHE_MAX_COST":               "4MiB",
			// Dispatch to ourselves over the cluster gRPC port so the
			// dispatch-cluster cache is exercised too, not just the local one.
			"SPICEDB_DISPATCH_CLUSTER_ENABLED": "true",
			"SPICEDB_DISPATCH_UPSTREAM_ADDR":   spicedbNetworkAlias + ":50053",
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = spicedb.Terminate(t.Context()) })

	client, err := authzed.NewClient(spicedb.GRPCEndpoint(),
		grpcutil.WithInsecureBearerToken(spicedb.PresharedKey()),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	writeSchemaWithRetry(ctx, t, client)

	numRels := envIntOr(t, "CACHE_FILL_DOCS", 10_000)

	writeRelationships(ctx, t, client, numRels)

	// Bumping the head revision each round makes the fully-consistent checks
	// dispatch on fresh cache keys, so every round adds cache entries that
	// (given the long quantization TTL) never expire during the run. A server
	// that evicts at its budget plateaus below GOMEMLIMIT; one that lets its
	// caches grow past their budget eventually crosses it and starts rejecting.
	rounds := envIntOr(t, "CACHE_FILL_ROUNDS", 20)
	for round := 1; round <= rounds; round++ {
		bumpRevision(ctx, t, client, round)
		checkAllRelationships(ctx, t, client, numRels, round)
		t.Logf("round %d/%d: all %d checks returned HAS_PERMISSION",
			round, rounds, numRels)
	}
}

// bumpRevision touches a sentinel relationship so the head revision moves,
// forcing the next round of fully-consistent checks onto fresh cache keys.
func bumpRevision(ctx context.Context, t *testing.T, client *authzed.Client, round int) {
	t.Helper()
	_, err := client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{{
			Operation: v1.RelationshipUpdate_OPERATION_TOUCH,
			Relationship: &v1.Relationship{
				Resource: &v1.ObjectReference{
					ObjectType: "group",
					ObjectId:   "revision-bump",
				},
				Relation: "member",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   fmt.Sprintf("bump-%d", round),
					},
				},
			},
		}},
	})
	require.NoError(t, err, "round %d: failed to bump revision", round)
}

func checkAllRelationships(ctx context.Context, t *testing.T, client *authzed.Client, numRels, round int) {
	t.Helper()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(32)
	for i := 0; i < numRels; i++ {
		g.Go(func() error {
			resp, err := client.CheckPermission(gctx, &v1.CheckPermissionRequest{
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{
						FullyConsistent: true,
					},
				},
				Resource:   docRef(i),
				Permission: "view",
				Subject:    &v1.SubjectReference{Object: userRef(i)},
			})
			if err != nil {
				if status.Code(err) == codes.ResourceExhausted {
					return fmt.Errorf(
						"round %d: check for doc-%d rejected with "+
							"RESOURCE_EXHAUSTED (%w) — the server's heap crossed "+
							"GOMEMLIMIT under cache-filling load",
						round, i, err)
				}
				return fmt.Errorf("round %d: check for doc-%d failed: %w",
					round, i, err)
			}
			if resp.Permissionship !=
				v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
				return fmt.Errorf(
					"round %d: check for doc-%d returned %s, expected "+
						"HAS_PERMISSION", round, i, resp.Permissionship)
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())
}

func writeSchemaWithRetry(ctx context.Context, t *testing.T, client *authzed.Client) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := client.WriteSchema(ctx, &v1.WriteSchemaRequest{Schema: schemaText()})
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			require.NoError(t, err, "failed to write schema")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func writeRelationships(ctx context.Context, t *testing.T, client *authzed.Client, numDocs int) {
	t.Helper()
	const batchSize = 500
	updates := make([]*v1.RelationshipUpdate, 0, batchSize)
	flush := func(lo, hi int) {
		if len(updates) == 0 {
			return
		}
		_, err := client.WriteRelationships(ctx,
			&v1.WriteRelationshipsRequest{Updates: updates})
		require.NoError(t, err, "failed writing relationships for docs [%d, %d)", lo, hi)
		updates = updates[:0]
	}
	batchLo := 0
	for i := 0; i < numDocs; i++ {
		for k := 0; k < spicedbFanout; k++ {
			updates = append(updates, touch(docArmRel(i, k)))
		}
		// Only the last group has the member, so view resolves via arm
		// fanout-1 after the earlier arms miss.
		updates = append(updates, touch(groupMemberRel(i, spicedbFanout-1)))
		if len(updates) >= batchSize {
			flush(batchLo, i+1)
			batchLo = i + 1
		}
	}
	flush(batchLo, numDocs)
}

func touch(rel *v1.Relationship) *v1.RelationshipUpdate {
	return &v1.RelationshipUpdate{
		Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
		Relationship: rel,
	}
}

// docArmRel is document:doc-i#arm{k}@group:grp-i-k#member.
func docArmRel(i, k int) *v1.Relationship {
	return &v1.Relationship{
		Resource: docRef(i),
		Relation: fmt.Sprintf("arm%d", k),
		Subject: &v1.SubjectReference{
			Object:           groupRef(i, k),
			OptionalRelation: "member",
		},
	}
}

// groupMemberRel is group:grp-i-k#member@user:user-i.
func groupMemberRel(i, k int) *v1.Relationship {
	return &v1.Relationship{
		Resource: groupRef(i, k),
		Relation: "member",
		Subject:  &v1.SubjectReference{Object: userRef(i)},
	}
}

func docRef(i int) *v1.ObjectReference {
	return &v1.ObjectReference{ObjectType: "document", ObjectId: fmt.Sprintf("doc-%d", i)}
}

func groupRef(i, k int) *v1.ObjectReference {
	return &v1.ObjectReference{ObjectType: "group", ObjectId: fmt.Sprintf("grp-%d-%d", i, k)}
}

func userRef(i int) *v1.ObjectReference {
	return &v1.ObjectReference{ObjectType: "user", ObjectId: fmt.Sprintf("user-%d", i)}
}

// schemaText builds a schema whose document.view is a union of `fanout`
// relations, each pointing at a group's membership.
func schemaText() string {
	var b strings.Builder
	b.WriteString("definition user {}\n\n")
	b.WriteString("definition group {\n\trelation member: user\n}\n\n")
	b.WriteString("definition document {\n")
	arms := make([]string, 0, spicedbFanout)
	for k := 0; k < spicedbFanout; k++ {
		fmt.Fprintf(&b, "\trelation arm%d: group#member\n", k)
		arms = append(arms, fmt.Sprintf("arm%d", k))
	}
	fmt.Fprintf(&b, "\tpermission view = %s\n}", strings.Join(arms, " + "))
	return b.String()
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func envIntOr(t *testing.T, name string, fallback int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	require.NoError(t, err, "invalid %s", name)
	return n
}

func inNetworkURI(t *testing.T, hostURI string) string {
	t.Helper()
	u, err := url.Parse(hostURI)
	require.NoError(t, err)
	u.Host = pgNetworkAlias + ":5432"
	return u.String()
}
