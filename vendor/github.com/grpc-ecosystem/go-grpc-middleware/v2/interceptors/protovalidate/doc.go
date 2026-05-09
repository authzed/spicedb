// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.

/*
Package protovalidate is a request validator that uses https://github.com/bufbuild/protovalidate-go under the hood.

In case of a validation failure, an `InvalidArgument` gRPC status is returned, along with a
description of the validation failure.

It supports two ways of work:

1. use new annotations that will be caught and processed by `protovalidate-go` package.

2. use legacy mode, annotations will be same as for `protoc-gen-validate` and `Validate()` method will be generated.

Example of a service:

	syntax = "proto3";
	package cloud.instance.v1;

	import "buf/validate/validate.proto";
	import "validate/validate.proto";

	service InstanceService {
	 // GetInstance is an example of request that uses a new constraints
	 rpc GetInstance(GetInstanceRequest) returns (GetInstanceResponse) {}

	 // Legacy is an example of request that uses protoc-gen-validate constraints
	 // their support enabled in protovalidate library constructor
	 rpc Legacy(LegacyRequest) returns (LegacyResponse) {}
	}

	message GetInstanceRequest {
	 string instance_id = 1 [(buf.validate.field).string.uuid = true];
	}

	message GetInstanceResponse {}

	message LegacyRequest {
	 string email = 1 [(validate.rules).string.email = true]; // https://github.com/bufbuild/protoc-gen-validate
	}

	message LegacyResponse {}

Please consult https://github.com/bufbuild/protovalidate for details and other parameters of customization.
*/
package protovalidate
