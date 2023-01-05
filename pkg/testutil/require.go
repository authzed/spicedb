// Package testutil implements various utilities to reduce boilerplate in unit
// tests a la testify.
package testutil

import (
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// RequireEqualEmptyNil is a version of require.Equal, but considers nil
// slices/maps to be equal to empty slices/maps.
func RequireEqualEmptyNil(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	opts := []cmp.Option{
		cmpopts.IgnoreUnexported(
			v0.RelationTuple{},
			v0.ObjectAndRelation{},
			v0.RelationReference{},
			v0.User_Userset{},
			v0.User{},
			v0.EditCheckResult{},
			v0.EditCheckResultValidationError{},
			v0.DeveloperError{},
			core.RelationTuple{},
			core.ObjectAndRelation{},
			core.RelationReference{},
			core.Relation{},
			core.RelationTupleTreeNode_IntermediateNode{},
			core.RelationTupleTreeNode{},
			core.RelationTupleTreeNode_LeafNode{},
			core.DirectSubjects{},
			core.SetOperationUserset{}),
		cmpopts.EquateEmpty(),
	}

	msgAndArgs = append(msgAndArgs, cmp.Diff(expected, actual, opts...))
	require.Truef(t, cmp.Equal(expected, actual, opts...), "Should be equal", msgAndArgs...)
}

// RequireWithin requires that the runner complete its execution within the specified duration.
func RequireWithin(t *testing.T, runner func(t *testing.T), timeout time.Duration) {
	t.Helper()

	ch := make(chan bool, 1)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	go (func() {
		t.Helper()
		runner(t)
		ch <- true
	})()

	select {
	case <-timer.C:
		require.Failf(t, "timed out waiting for runner", "expected to complete in %v", timeout)

	case <-ch:
		timer.Stop()
		return
	}
}
