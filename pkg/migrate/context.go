package migrate

import "github.com/authzed/ctxkey"

// BackfillBatchSize is the context key for the number of items that should be
// backfilled in a single step of an incremental backfill.
var BackfillBatchSize = ctxkey.New[uint64]()
