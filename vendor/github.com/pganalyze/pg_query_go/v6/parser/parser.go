package parser

/*
#cgo CFLAGS: -Iinclude -Iinclude/postgres -g -fstack-protector -std=gnu99 -Wno-unknown-warning-option
#cgo windows CFLAGS: -Iinclude/postgres/port/win32
#cgo LDFLAGS:
#include "pg_query.h"
#include "xxhash.h"
#include <stdlib.h>

// Avoid complexities dealing with C structs in Go
PgQueryDeparseResult pg_query_deparse_protobuf_direct_args(void* data, unsigned int len) {
	PgQueryProtobuf p;
	p.data = (char *) data;
	p.len = len;
	return pg_query_deparse_protobuf(p);
}

// Avoid inconsistent type behaviour in xxhash library
uint64_t pg_query_hash_xxh3_64(void *data, size_t len, size_t seed) {
	return XXH3_64bits_withSeed(data, len, seed);
}
*/
import "C"

import (
	"strings"
	"unsafe"
)

func init() {
	C.pg_query_init()
}

type Error struct {
	Message   string // exception message
	Funcname  string // source function of exception (e.g. SearchSysCache)
	Filename  string // source of exception (e.g. parse.l)
	Lineno    int    // source of exception (e.g. 104)
	Cursorpos int    // char in query at which exception occurred
	Context   string // additional context (optional, can be NULL)
}

func (e *Error) Error() string {
	return e.Message
}

func newPgQueryError(errC *C.PgQueryError) *Error {
	err := &Error{
		Message:   C.GoString(errC.message),
		Lineno:    int(errC.lineno),
		Cursorpos: int(errC.cursorpos),
	}
	if errC.funcname != nil {
		err.Funcname = C.GoString(errC.funcname)
	}
	if errC.filename != nil {
		err.Filename = C.GoString(errC.filename)
	}
	if errC.context != nil {
		err.Context = C.GoString(errC.context)
	}
	return err
}

// ParseToJSON - Parses the given SQL statement into a parse tree (JSON format)
func ParseToJSON(input string) (result string, err error) {
	inputC := C.CString(input)
	defer C.free(unsafe.Pointer(inputC))

	resultC := C.pg_query_parse(inputC)

	defer C.pg_query_free_parse_result(resultC)

	if resultC.error != nil {
		err = newPgQueryError(resultC.error)
		return
	}

	result = C.GoString(resultC.parse_tree)

	return
}

// Scans the given SQL statement into a protobuf ScanResult
func ScanToProtobuf(input string) (result []byte, err error) {
	inputC := C.CString(input)
	defer C.free(unsafe.Pointer(inputC))

	resultC := C.pg_query_scan(inputC)
	defer C.pg_query_free_scan_result(resultC)

	if resultC.error != nil {
		err = newPgQueryError(resultC.error)
		return
	}

	result = []byte(C.GoStringN(resultC.pbuf.data, C.int(resultC.pbuf.len)))

	return
}

// ParseToProtobuf - Parses the given SQL statement into a parse tree (Protobuf format)
func ParseToProtobuf(input string) (result []byte, err error) {
	inputC := C.CString(input)
	defer C.free(unsafe.Pointer(inputC))

	resultC := C.pg_query_parse_protobuf(inputC)

	defer C.pg_query_free_protobuf_parse_result(resultC)

	if resultC.error != nil {
		err = newPgQueryError(resultC.error)
		return
	}

	result = []byte(C.GoStringN(resultC.parse_tree.data, C.int(resultC.parse_tree.len)))

	return
}

// DeparseFromProtobuf - Deparses the given Protobuf format parse tree into a SQL statement
func DeparseFromProtobuf(input []byte) (result string, err error) {
	inputC := C.CBytes(input)
	defer C.free(inputC)

	resultC := C.pg_query_deparse_protobuf_direct_args(inputC, C.uint(len(input)))

	defer C.pg_query_free_deparse_result(resultC)

	if resultC.error != nil {
		err = newPgQueryError(resultC.error)
		return
	}

	result = C.GoString(resultC.query)

	return
}

// ParsePlPgSqlToJSON - Parses the given PL/pgSQL function statement into a parse tree (JSON format)
func ParsePlPgSqlToJSON(input string) (result string, err error) {
	inputC := C.CString(input)
	defer C.free(unsafe.Pointer(inputC))

	resultC := C.pg_query_parse_plpgsql(inputC)

	defer C.pg_query_free_plpgsql_parse_result(resultC)

	if resultC.error != nil {
		err = newPgQueryError(resultC.error)
		return
	}

	result = C.GoString(resultC.plpgsql_funcs)

	return
}

// Normalize the passed SQL statement to replace constant values with ? characters
func Normalize(input string) (result string, err error) {
	inputC := C.CString(input)
	defer C.free(unsafe.Pointer(inputC))

	resultC := C.pg_query_normalize(inputC)
	defer C.pg_query_free_normalize_result(resultC)

	if resultC.error != nil {
		err = newPgQueryError(resultC.error)
		return
	}

	result = C.GoString(resultC.normalized_query)

	return
}

// Normalize the passed utility statement to replace constant values with ? characters
func NormalizeUtility(input string) (result string, err error) {
	inputC := C.CString(input)
	defer C.free(unsafe.Pointer(inputC))

	resultC := C.pg_query_normalize_utility(inputC)
	defer C.pg_query_free_normalize_result(resultC)

	if resultC.error != nil {
		err = newPgQueryError(resultC.error)
		return
	}

	result = C.GoString(resultC.normalized_query)

	return
}

func SplitWithScanner(input string, trimSpace bool) (result []string, err error) {
	inputC := C.CString(input)
	defer C.free(unsafe.Pointer(inputC))

	resultC := C.pg_query_split_with_scanner(inputC)
	defer C.pg_query_free_split_result(resultC)

	if resultC.error != nil {
		err = newPgQueryError(resultC.error)
		return
	}

	result = handleSplitResult(input, trimSpace, resultC)
	return
}

func SplitWithParser(input string, trimSpace bool) (result []string, err error) {
	inputC := C.CString(input)
	defer C.free(unsafe.Pointer(inputC))

	resultC := C.pg_query_split_with_parser(inputC)
	defer C.pg_query_free_split_result(resultC)

	if resultC.error != nil {
		err = newPgQueryError(resultC.error)
		return
	}

	result = handleSplitResult(input, trimSpace, resultC)
	return
}

func handleSplitResult(input string, trimSpace bool, resultC C.PgQuerySplitResult) (result []string) {
	stmts := (**C.PgQuerySplitStmt)(unsafe.Pointer(resultC.stmts))
	for i := 0; i < int(resultC.n_stmts); i++ {
		stmtptr := (**C.PgQuerySplitStmt)(unsafe.Pointer(uintptr(unsafe.Pointer(stmts)) + uintptr(i)*unsafe.Sizeof(*stmts)))
		stmt := **stmtptr

		end := stmt.stmt_location + stmt.stmt_len
		stmtStr := input[stmt.stmt_location:end]
		if trimSpace {
			stmtStr = strings.TrimSpace(stmtStr)
		}

		result = append(result, stmtStr)
	}
	return
}

// FingerprintToUInt64 - Fingerprint the passed SQL statement using the C extension and returns result as uint64
func FingerprintToUInt64(input string) (result uint64, err error) {
	inputC := C.CString(input)
	defer C.free(unsafe.Pointer(inputC))

	resultC := C.pg_query_fingerprint(inputC)
	defer C.pg_query_free_fingerprint_result(resultC)

	if resultC.error != nil {
		err = newPgQueryError(resultC.error)
		return
	}

	// https://github.com/golang/go/issues/29878
	result = *(*uint64)(unsafe.Pointer(&resultC.fingerprint))

	return
}

// FingerprintToHexStr - Fingerprint the passed SQL statement using the C extension and returns result as hex string
func FingerprintToHexStr(input string) (result string, err error) {
	inputC := C.CString(input)
	defer C.free(unsafe.Pointer(inputC))

	resultC := C.pg_query_fingerprint(inputC)
	defer C.pg_query_free_fingerprint_result(resultC)

	if resultC.error != nil {
		err = newPgQueryError(resultC.error)
		return
	}

	result = C.GoString(resultC.fingerprint_str)

	return
}

// HashXXH3_64 - Helper method to run XXH3 hash function (64-bit variant) on the given bytes, with the specified seed
func HashXXH3_64(input []byte, seed uint64) (result uint64) {
	inputC := C.CBytes(input)
	defer C.free(inputC)

	res := C.pg_query_hash_xxh3_64(inputC, C.size_t(len(input)), C.size_t(seed))

	// https://github.com/golang/go/issues/29878
	result = *(*uint64)(unsafe.Pointer(&res))

	return
}
