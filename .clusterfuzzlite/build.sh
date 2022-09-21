#!/bin/bash -eu

go install github.com/AdamKorcz/go-118-fuzz-build@latest
go get github.com/AdamKorcz/go-118-fuzz-build/utils

# Workaround https://github.com/AdamKorcz/go-118-fuzz-build/issues/2
mv $SRC/spicedb/pkg/schemadsl/parser/parser_test.go $SRC/spicedb/pkg/schemadsl/parser/parser_test_fuzz.go

compile_native_go_fuzzer github.com/authzed/spicedb/pkg/schemadsl/parser FuzzParser FuzzParser

