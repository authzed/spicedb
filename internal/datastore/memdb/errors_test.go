package memdb

import (
	"fmt"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func TestErrors(t *testing.T) {
	err := NewSerializationMaxRetriesReachedErr(fmt.Errorf("some error"))
	spiceerrors.RequireReason(t, v1.ErrorReason_ERROR_REASON_UNSPECIFIED, err, "details")
}
