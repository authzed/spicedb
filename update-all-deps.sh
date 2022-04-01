#! /bin/sh

set -x 

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR
go get -u ./...
go mod tidy
cd $SCRIPT_DIR/e2e
go get -u ./...
go mod tidy
