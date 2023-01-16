#!/bin/bash
set -xeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

cd "$SCRIPT_DIR/.." || exit
go get -u -t -tags ci,tools ./...
go mod tidy

cd "$SCRIPT_DIR/../e2e" || exit
go get -u -t -u -t -tags tools ./...
go mod tidy
