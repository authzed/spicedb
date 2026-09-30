//go:build integration

package integrationtesting_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/cmd/server"
)

func TestRelationshipSetCacheServesChecks(t *testing.T) {
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)
	ds, _ := testfixtures.StandardDatastoreWithData(t, rawDS)

	conns := testserver.TestClusterWithDispatch(t, 1, ds,
		server.WithEnableExperimentalRelationshipSetCache(true),
		server.WithRelationshipSetCacheMaterializeThreshold(1),
	)
	client := v1.NewPermissionsServiceClient(conns[0])

	// Materialized sets serve the later rounds.
	// document:companyplan -> parent folder:company -> viewer folder:auditors#viewer -> user:auditor.
	for range 3 {
		resp, err := client.CheckPermission(t.Context(), &v1.CheckPermissionRequest{
			Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
			Resource:    &v1.ObjectReference{ObjectType: "document", ObjectId: "companyplan"},
			Permission:  "view",
			Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "auditor"}},
		})
		require.NoError(t, err)
		require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, resp.Permissionship)

		resp, err = client.CheckPermission(t.Context(), &v1.CheckPermissionRequest{
			Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
			Resource:    &v1.ObjectReference{ObjectType: "document", ObjectId: "companyplan"},
			Permission:  "view",
			Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "villain"}},
		})
		require.NoError(t, err)
		require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, resp.Permissionship)
	}
}
