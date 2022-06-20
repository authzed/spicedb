package dispatch

import (
	"errors"
	"testing"

	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/stretchr/testify/require"
)

type innerError struct {
	error
}

func TestErrorUnwrap(t *testing.T) {
	require := require.New(t)

	inner := innerError{
		errors.New("inner error"),
	}

	wrapped := WrapWithMetadata(inner, &dispatchv1.ResponseMeta{
		DispatchCount:       6,
		DepthRequired:       7,
		CachedDispatchCount: 8,
	})

	var asWrapped *metadataErr
	require.True(errors.As(wrapped, &asWrapped))
	require.Equal(uint32(6), asWrapped.GetMetadata().DispatchCount)
	require.Equal("inner error", asWrapped.Error())

	var asInner innerError
	require.True(errors.As(wrapped, &asInner))
	require.Equal("inner error", asInner.Error())
}
