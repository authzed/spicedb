package postgres

import (
	"context"
	"github.com/authzed/spicedb/pkg/middleware/tenantid"
)

func tenantIDFromContext(ctx context.Context) string {
	value := ctx.Value(tenantid.CtxTenantIDKey)
	if value == nil {
		return ""
	}
	return value.(string)
}
