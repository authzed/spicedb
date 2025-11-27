package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/middleware/memoryprotection"
	"github.com/authzed/spicedb/internal/middleware/pertoken"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
)

func TestWithDatastore(t *testing.T) {
	someLogger := zerolog.Nop()
	someAuthFunc := func(ctx context.Context) (context.Context, error) {
		return nil, fmt.Errorf("expected auth error")
	}
	var someDispatcher dispatch.Dispatcher

	opts := MiddlewareOption{
		someLogger,
		someAuthFunc,
		true,
		someDispatcher,
		true,
		true,
		false,
		"service",
		consistency.TreatMismatchingTokensAsError,
		memoryprotection.NewNoopMemoryUsageProvider(),
		nil,
		nil,
	}

	someDS, err := memdb.NewMemdbDatastore(0, time.Hour, time.Hour)
	require.NoError(t, err)

	withDS := opts.WithDatastore(someDS)
	require.NotNil(t, withDS)
	require.NotNil(t, withDS.unaryDatastoreMiddleware)
	require.NotNil(t, withDS.streamDatastoreMiddleware)

	require.Equal(t, opts.Logger, withDS.Logger)
	require.Equal(t, opts.DispatcherForMiddleware, withDS.DispatcherForMiddleware)
	require.Equal(t, opts.EnableRequestLog, withDS.EnableRequestLog)
	require.Equal(t, opts.EnableResponseLog, withDS.EnableResponseLog)
	require.Equal(t, opts.DisableGRPCHistogram, withDS.DisableGRPCHistogram)
	require.Equal(t, opts.MiddlewareServiceLabel, withDS.MiddlewareServiceLabel)
	require.Equal(t, opts.MismatchingZedTokenOption, withDS.MismatchingZedTokenOption)

	_, authError := withDS.AuthFunc(t.Context())
	require.Error(t, authError)
	require.ErrorContains(t, authError, "expected auth error")
}

func TestWithDatastoreMiddleware(t *testing.T) {
	someLogger := zerolog.Nop()
	someAuthFunc := func(ctx context.Context) (context.Context, error) {
		return nil, fmt.Errorf("expected auth error")
	}
	var someDispatcher dispatch.Dispatcher

	opts := MiddlewareOption{
		someLogger,
		someAuthFunc,
		true,
		someDispatcher,
		true,
		true,
		false,
		"service",
		consistency.TreatMismatchingTokensAsError,
		memoryprotection.NewNoopMemoryUsageProvider(),
		nil,
		nil,
	}

	someMiddleware := pertoken.NewMiddleware(nil, caveattypes.Default.TypeSet)

	withDS := opts.WithDatastoreMiddleware(someMiddleware)
	require.NotNil(t, withDS)
	require.NotNil(t, withDS.unaryDatastoreMiddleware)
	require.NotNil(t, withDS.streamDatastoreMiddleware)

	require.Equal(t, opts.Logger, withDS.Logger)
	require.Equal(t, opts.DispatcherForMiddleware, withDS.DispatcherForMiddleware)
	require.Equal(t, opts.EnableRequestLog, withDS.EnableRequestLog)
	require.Equal(t, opts.EnableResponseLog, withDS.EnableResponseLog)
	require.Equal(t, opts.DisableGRPCHistogram, withDS.DisableGRPCHistogram)
	require.Equal(t, opts.MiddlewareServiceLabel, withDS.MiddlewareServiceLabel)
	require.Equal(t, opts.MismatchingZedTokenOption, withDS.MismatchingZedTokenOption)

	_, authError := withDS.AuthFunc(t.Context())
	require.Error(t, authError)
	require.ErrorContains(t, authError, "expected auth error")
}
