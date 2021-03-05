#!/bin/bash
shopt -s extglob

# Change to the project's root directory
SCRIPTS_PATH=$( cd "$(dirname "${BASH_SOURCE[0]}")" || exit ; pwd -P )
cd "$SCRIPTS_PATH"/../.. || exit

OUT_PATH="spicedb/pkg/"
# shellcheck disable=SC2046
protoc \
  -I=protos --go_out=$OUT_PATH --go-grpc_out=$OUT_PATH \
  --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative \
  protos/REDACTEDapi/api/core.proto $(ls protos/REDACTEDapi/api/!(core).proto) \
$(ls protos/REDACTEDapi/impl/*.proto) \
$(ls protos/REDACTEDapi/healthcheck/*.proto) 
