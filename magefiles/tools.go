//go:build tools
// +build tools

package tools

import (
	_ "github.com/agnivade/wasmbrowsertest"
	_ "github.com/bufbuild/buf/cmd/buf"
	_ "github.com/ecordell/optgen"
	_ "github.com/envoyproxy/protoc-gen-validate"
	_ "github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto"
	_ "golang.org/x/tools/cmd/stringer"
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
	_ "mvdan.cc/gofumpt"
)
