#ifndef PG_QUERY_H
#define PG_QUERY_H

#include <stdint.h>
#include <sys/types.h>

typedef struct {
	char* message; // exception message
	char* funcname; // source function of exception (e.g. SearchSysCache)
	char* filename; // source of exception (e.g. parse.l)
	int lineno; // source of exception (e.g. 104)
	int cursorpos; // char in query at which exception occurred
	char* context; // additional context (optional, can be NULL)
} PgQueryError;

typedef struct {
  size_t len;
  char* data;
} PgQueryProtobuf;

typedef struct {
  PgQueryProtobuf pbuf;
  char* stderr_buffer;
  PgQueryError* error;
} PgQueryScanResult;

typedef struct {
  char* parse_tree;
  char* stderr_buffer;
  PgQueryError* error;
} PgQueryParseResult;

typedef struct {
  PgQueryProtobuf parse_tree;
  char* stderr_buffer;
  PgQueryError* error;
} PgQueryProtobufParseResult;

typedef struct {
  int stmt_location;
  int stmt_len;
} PgQuerySplitStmt;

typedef struct {
  PgQuerySplitStmt **stmts;
  int n_stmts;
  char* stderr_buffer;
  PgQueryError* error;
} PgQuerySplitResult;

typedef struct {
  char* query;
  PgQueryError* error;
} PgQueryDeparseResult;

typedef struct {
  char* plpgsql_funcs;
  PgQueryError* error;
} PgQueryPlpgsqlParseResult;

typedef struct {
  uint64_t fingerprint;
  char* fingerprint_str;
  char* stderr_buffer;
  PgQueryError* error;
} PgQueryFingerprintResult;

typedef struct {
  char* normalized_query;
  PgQueryError* error;
} PgQueryNormalizeResult;

// Postgres parser options (parse mode and GUCs that affect parsing)

typedef enum
{
	PG_QUERY_PARSE_DEFAULT = 0,
	PG_QUERY_PARSE_TYPE_NAME,
	PG_QUERY_PARSE_PLPGSQL_EXPR,
	PG_QUERY_PARSE_PLPGSQL_ASSIGN1,
	PG_QUERY_PARSE_PLPGSQL_ASSIGN2,
	PG_QUERY_PARSE_PLPGSQL_ASSIGN3
} PgQueryParseMode;

// We technically only need 3 bits to store parse mode, but
// having 4 bits avoids API breaks if another one gets added.
#define PG_QUERY_PARSE_MODE_BITS 4
#define PG_QUERY_PARSE_MODE_BITMASK ((1 << PG_QUERY_PARSE_MODE_BITS) - 1)

#define PG_QUERY_DISABLE_BACKSLASH_QUOTE 16 // backslash_quote = off (default is safe_encoding, which is effectively on)
#define PG_QUERY_DISABLE_STANDARD_CONFORMING_STRINGS 32 // standard_conforming_strings = off (default is on)
#define PG_QUERY_DISABLE_ESCAPE_STRING_WARNING 64 // escape_string_warning = off (default is on)

#ifdef __cplusplus
extern "C" {
#endif

PgQueryNormalizeResult pg_query_normalize(const char* input);
PgQueryNormalizeResult pg_query_normalize_utility(const char* input);
PgQueryScanResult pg_query_scan(const char* input);
PgQueryParseResult pg_query_parse(const char* input);
PgQueryParseResult pg_query_parse_opts(const char* input, int parser_options);
PgQueryProtobufParseResult pg_query_parse_protobuf(const char* input);
PgQueryProtobufParseResult pg_query_parse_protobuf_opts(const char* input, int parser_options);
PgQueryPlpgsqlParseResult pg_query_parse_plpgsql(const char* input);

PgQueryFingerprintResult pg_query_fingerprint(const char* input);
PgQueryFingerprintResult pg_query_fingerprint_opts(const char* input, int parser_options);

// Use pg_query_split_with_scanner when you need to split statements that may
// contain parse errors, otherwise pg_query_split_with_parser is recommended
// for improved accuracy due the parser adding additional token handling.
//
// Note that we try to support special cases like comments, strings containing
// ";" on both, as well as oddities like "CREATE RULE .. (SELECT 1; SELECT 2);"
// which is treated as as single statement.
PgQuerySplitResult pg_query_split_with_scanner(const char *input);
PgQuerySplitResult pg_query_split_with_parser(const char *input);

PgQueryDeparseResult pg_query_deparse_protobuf(PgQueryProtobuf parse_tree);

void pg_query_free_normalize_result(PgQueryNormalizeResult result);
void pg_query_free_scan_result(PgQueryScanResult result);
void pg_query_free_parse_result(PgQueryParseResult result);
void pg_query_free_split_result(PgQuerySplitResult result);
void pg_query_free_deparse_result(PgQueryDeparseResult result);
void pg_query_free_protobuf_parse_result(PgQueryProtobufParseResult result);
void pg_query_free_plpgsql_parse_result(PgQueryPlpgsqlParseResult result);
void pg_query_free_fingerprint_result(PgQueryFingerprintResult result);

// Optional, cleans up the top-level memory context (automatically done for threads that exit)
void pg_query_exit(void);

// Postgres version information
#define PG_MAJORVERSION "17"
#define PG_VERSION "17.4"
#define PG_VERSION_NUM 170004

// Deprecated APIs below

void pg_query_init(void); // Deprecated as of 9.5-1.4.1, this is now run automatically as needed

#ifdef __cplusplus
}
#endif

#endif
