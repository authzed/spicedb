package v0

import "os"

func init() {
	// If a binary imports both SpiceDB and authzed-go, this ignores name
	// conflicts, as they are compatible protos.
	os.Setenv("GOLANG_PROTOBUF_REGISTRATION_CONFLICT", "ignore")
}
