package tables

import (
	"fmt"

	"google.golang.org/grpc/metadata"

	"github.com/authzed/authzed-go/pkg/responsemeta"
)

func costFromTrailers(trailersMD metadata.MD) (float32, error) {
	dispatchCount, trailerErr := responsemeta.GetIntResponseTrailerMetadata(
		trailersMD,
		responsemeta.DispatchedOperationsCount,
	)
	if trailerErr != nil {
		return 0, fmt.Errorf("error reading dispatched operations trailer: %w", trailerErr)
	}

	cachedCount, trailerErr := responsemeta.GetIntResponseTrailerMetadata(
		trailersMD,
		responsemeta.CachedOperationsCount,
	)
	if trailerErr != nil {
		return 0, fmt.Errorf("error reading dispatched operations trailer: %w", trailerErr)
	}

	return float32(dispatchCount + cachedCount), nil
}
