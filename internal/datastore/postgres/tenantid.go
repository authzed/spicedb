package postgres

import "context"

type tenantIDKeyType string

const tenantIDKey tenantIDKeyType = "tenantID"

func tenantIDFromContext(ctx context.Context) string {
	value := ctx.Value(tenantIDKey)
	if value == nil {
		return ""
	}
	return value.(string)
}
