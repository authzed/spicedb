package pertoken

import (
	"context"
	"fmt"
	"sync"
	"time"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const (
	gcWindow             = 1 * time.Hour
	revisionQuantization = 10 * time.Millisecond
)

// MiddlewareForTesting is used to create a unique datastore for each token. It is intended for use in the
// testserver only.
type MiddlewareForTesting struct {
	datastoreByToken *sync.Map
	configFilePaths  []string
}

// NewMiddleware returns a new per-token datastore middleware that initializes each datastore with the data in the
// config files.
func NewMiddleware(configFilePaths []string) *MiddlewareForTesting {
	return &MiddlewareForTesting{
		datastoreByToken: &sync.Map{},
		configFilePaths:  configFilePaths,
	}
}

type squashable interface {
	SquashRevisionsForTesting()
}

func (m *MiddlewareForTesting) getOrCreateDatastore(ctx context.Context) (datastore.Datastore, error) {
	tokenStr, _ := grpcauth.AuthFromMD(ctx, "bearer")
	tokenDatastore, ok := m.datastoreByToken.Load(tokenStr)
	if ok {
		return tokenDatastore.(datastore.Datastore), nil
	}

	log.Ctx(ctx).Debug().Str("token", tokenStr).Msg("initializing new upstream for token")
	ds, err := memdb.NewMemdbDatastore(0, revisionQuantization, gcWindow)
	if err != nil {
		return nil, fmt.Errorf("failed to init datastore: %w", err)
	}

	_, _, err = validationfile.PopulateFromFiles(ctx, ds, m.configFilePaths)
	if err != nil {
		return nil, fmt.Errorf("failed to load config files: %w", err)
	}

	// Squash the revisions so that the caller sees all the populated data.
	ds.(squashable).SquashRevisionsForTesting()

	m.datastoreByToken.Store(tokenStr, ds)
	return ds, nil
}

// UnaryServerInterceptor returns a new unary server interceptor that sets a separate in-memory datastore per token
func (m *MiddlewareForTesting) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		tokenDatastore, err := m.getOrCreateDatastore(ctx)
		if err != nil {
			return nil, err
		}

		newCtx := datastoremw.ContextWithHandle(ctx)
		if err := datastoremw.SetInContext(newCtx, tokenDatastore); err != nil {
			return nil, err
		}

		return handler(newCtx, req)
	}
}

// StreamServerInterceptor returns a new stream server interceptor that sets a separate in-memory datastore per token
func (m *MiddlewareForTesting) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		tokenDatastore, err := m.getOrCreateDatastore(stream.Context())
		if err != nil {
			return err
		}

		wrapped := middleware.WrapServerStream(stream)
		wrapped.WrappedContext = datastoremw.ContextWithHandle(wrapped.WrappedContext)
		if err := datastoremw.SetInContext(wrapped.WrappedContext, tokenDatastore); err != nil {
			return err
		}
		return handler(srv, wrapped)
	}
}
