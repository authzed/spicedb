#!/bin/bash
set -xeuo pipefail

read -ra TOOLS < <(go list -f "{{range .Imports}}{{.}} {{end}}" tools.go)
go install "${TOOLS[@]}"
