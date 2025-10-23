package sharederrors

const (
	PostgresEnableWatchErrorLink = "https://spicedb.dev/d/enable-watch-api-postgres"
	MySQLParseErrorLink          = "https://spicedb.dev/d/parse-time-mysql"
	CrdbOverlapErrorLink         = "https://spicedb.dev/d/crdb-overlap"
	CrdbGcWindowErrorLink        = "https://spicedb.dev/d/crdb-gc-window-warning"
	CrdbEnableWatchErrorLink     = "https://spicedb.dev/d/enable-watch-api-crdb"
	QueryExecModeErrorLink       = "https://spicedb.dev/d/query-exec-mode"
	TruncateUnsupportedErrorLink = "https://spicedb.dev/d/truncate-unsupported"
	MaxDepthErrorLink            = "https://spicedb.dev/d/debug-max-depth"
)

var allErrorHelpUrls = []string{
	CrdbOverlapErrorLink,
	MySQLParseErrorLink,
	MaxDepthErrorLink,
	CrdbGcWindowErrorLink,
	CrdbEnableWatchErrorLink,
	PostgresEnableWatchErrorLink,
	QueryExecModeErrorLink,
	TruncateUnsupportedErrorLink,
}
