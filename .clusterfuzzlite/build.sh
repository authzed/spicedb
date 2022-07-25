#!/bin/bash -eu
mv $SRC/spicedb/pkg/schemadsl/parser/parser_test.go $SRC/spicedb/pkg/schemadsl/parser/parser_test_fuzz.go

compile_go_fuzzer github.com/authzed/spicedb/pkg/schemadsl/parser Fuzz fuzz
