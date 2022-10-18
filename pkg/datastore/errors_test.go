package datastore

import (
	"fmt"
	"testing"

	"github.com/authzed/spicedb/internal/logging"
)

func TestError(t *testing.T) {
	logging.Info().Err(ErrNamespaceNotFound{
		error:         fmt.Errorf("test"),
		namespaceName: "test/test",
	},
	).Msg("test")
}
