package dispatchstrategy

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

const override = "prefer_secondary(['tiger'], duration('50ms'))"

// recordingValidator records its calls and returns err.
type recordingValidator struct {
	expr  string
	kinds []string
	err   error
}

func (v *recordingValidator) Validate(expr string, kinds ...string) error {
	v.expr = expr
	v.kinds = kinds
	return v.err
}

func incoming(values ...string) context.Context {
	md := metadata.MD{}
	for _, value := range values {
		md.Append(metadataKey, value)
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestServerInterceptorValidation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		ctx           context.Context
		method        string
		validator     *recordingValidator
		expectedKinds []string
		expectedError string
	}{
		{
			name:      "no header",
			ctx:       context.Background(),
			method:    v1.PermissionsService_CheckPermission_FullMethodName,
			validator: nil,
		},
		{
			name:          "overrides disabled",
			ctx:           incoming(override),
			method:        v1.PermissionsService_CheckPermission_FullMethodName,
			validator:     nil,
			expectedError: "not enabled on this server",
		},
		{
			name:          "header set twice",
			ctx:           incoming(override, override),
			method:        v1.PermissionsService_CheckPermission_FullMethodName,
			validator:     &recordingValidator{},
			expectedError: "at most once",
		},
		{
			name:          "method without dispatches",
			ctx:           incoming(override),
			method:        v1.PermissionsService_WriteRelationships_FullMethodName,
			validator:     &recordingValidator{},
			expectedError: "not supported by /authzed.api.v1.PermissionsService/WriteRelationships",
		},
		{
			name:          "invalid override",
			ctx:           incoming(override),
			method:        v1.PermissionsService_CheckPermission_FullMethodName,
			validator:     &recordingValidator{err: errors.New("unknown secondary dispatcher `tiger`")},
			expectedKinds: []string{"check"},
			expectedError: "unknown secondary dispatcher `tiger`",
		},
		{
			name:          "check",
			ctx:           incoming(override),
			method:        v1.PermissionsService_CheckBulkPermissions_FullMethodName,
			validator:     &recordingValidator{},
			expectedKinds: []string{"check"},
		},
		{
			name:          "lookup resources also dispatches checks",
			ctx:           incoming(override),
			method:        v1.PermissionsService_LookupResources_FullMethodName,
			validator:     &recordingValidator{},
			expectedKinds: []string{"lookupresources", "check"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var validator Validator
			if tc.validator != nil {
				validator = tc.validator
			}

			called := false
			_, err := UnaryServerInterceptor(validator)(tc.ctx, nil, &grpc.UnaryServerInfo{FullMethod: tc.method}, func(context.Context, any) (any, error) {
				called = true
				return nil, nil
			})

			if tc.expectedError != "" {
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.ErrorContains(t, err, tc.expectedError)
				require.False(t, called)
			} else {
				require.NoError(t, err)
				require.True(t, called)
			}

			if tc.expectedKinds != nil {
				require.Equal(t, override, tc.validator.expr)
				require.Equal(t, tc.expectedKinds, tc.validator.kinds)
			}
		})
	}
}

func TestClientInterceptorForwardsOverride(t *testing.T) {
	t.Parallel()

	var outgoing metadata.MD
	invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		outgoing, _ = metadata.FromOutgoingContext(ctx)
		return nil
	}

	require.NoError(t, UnaryClientInterceptor()(incoming(override), "/dispatch", nil, nil, nil, invoker))
	require.Equal(t, []string{override}, outgoing.Get(metadataKey))

	outgoing = nil
	require.NoError(t, UnaryClientInterceptor()(context.Background(), "/dispatch", nil, nil, nil, invoker))
	require.Empty(t, outgoing.Get(metadataKey))
}

func TestPropagateIfExists(t *testing.T) {
	t.Parallel()

	target := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "abc"))
	propagated := PropagateIfExists(incoming(override), target)

	expr, ok := FromContext(propagated)
	require.True(t, ok)
	require.Equal(t, override, expr)
	require.Equal(t, []string{"abc"}, metadata.ValueFromIncomingContext(propagated, "x-request-id"))

	require.Equal(t, target, PropagateIfExists(context.Background(), target))
}
