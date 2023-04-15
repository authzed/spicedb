package datastore

import (
	"fmt"
	"testing"

	"github.com/authzed/spicedb/internal/logging"
)

func TestError(_ *testing.T) {
	logging.Info().Err(ErrNamespaceNotFound{
		error:         fmt.Errorf("test"),
		namespaceName: "test/test",
	},
	).Msg("test")
}
