package v1

import (
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
)

func TestInputMessagesForService(t *testing.T) {
	messages := inputMessagesForService(v1.RegisterPermissionsServiceServer, *new(v1.PermissionsServiceServer))
	require.NotEmpty(t, messages)
	// Ensure that a CheckPermissionsRequest is in the list
	checkPermRequestFound := false
	for _, message := range messages {
		if _, ok := message.(*v1.CheckPermissionRequest); ok {
			checkPermRequestFound = true
		}
	}
	require.True(t, checkPermRequestFound, "did not find a CheckPermissionRequest in the list")

	// Ensure that a LookupResourcesRequest is in the list
	lrRequestFound := false
	for _, message := range messages {
		if _, ok := message.(*v1.LookupResourcesRequest); ok {
			lrRequestFound = true
		}
	}
	require.True(t, lrRequestFound, "did not find a LookupResourcesRequest in the list")
}
