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
	"github.com/authzed/spicedb/internal/middleware/pertoken"
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

	_, authError := withDS.AuthFunc(context.Background())
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
		"anotherservice",
		nil,
		nil,
	}

	someMiddleware := pertoken.NewMiddleware(nil)

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

	_, authError := withDS.AuthFunc(context.Background())
	require.Error(t, authError)
	require.ErrorContains(t, authError, "expected auth error")
}
