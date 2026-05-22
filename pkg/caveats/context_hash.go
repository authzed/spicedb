package caveats

import (
	"encoding/base64"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	prefix = "ctx-"
)

// StableContextStringForHashing returns a stable string version of the context, for use in hashing.
func StableContextStringForHashing(context *structpb.Struct) (string, error) {
	// NOTE: using Deterministic: true guarantees that the same message will always be
	// serialized to the same bytes within the same binary:
	// https://pkg.go.dev/google.golang.org/protobuf/proto#MarshalOptions
	// This does mean that during a rollout, different SpiceDB instances running
	// different versions may disagree about how context is hashed; this slight
	// reduction in cache efficiency is offset by the guarantees that we get
	// around serialization correctness.
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(context)
	if err != nil {
		return "", err
	}
	return prefix + base64.RawStdEncoding.EncodeToString(encoded), nil
}
