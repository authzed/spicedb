package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query/signals"
)

func TestQuerySignalsOwnerInjection(t *testing.T) {
	shared := signals.NewDefaultOwner()
	a := NewPermissionsServer(nil, PermissionsServerConfig{QuerySignals: shared}, nil).(*permissionServer)
	b := NewPermissionsServer(nil, PermissionsServerConfig{QuerySignals: shared}, nil).(*permissionServer)
	require.Same(t, shared, a.querySignals)
	require.Same(t, shared, b.querySignals)
	c := NewPermissionsServer(nil, PermissionsServerConfig{}, nil).(*permissionServer)
	d := NewPermissionsServer(nil, PermissionsServerConfig{}, nil).(*permissionServer)
	require.NotSame(t, shared, c.querySignals)
	require.NotSame(t, c.querySignals, d.querySignals)
}
