package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

func TestInputMessagesForService(t *testing.T) {
	messages := inputMessagesForService(v1.RegisterPermissionsServiceServer, v1.PermissionsServiceServer(nil))
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

func TestInputMessagesForServiceOnWatch(t *testing.T) {
	messages := inputMessagesForService(v1.RegisterWatchServiceServer, v1.WatchServiceServer(nil))
	require.NotEmpty(t, messages)
	// Ensure that a CheckPermissionsRequest is in the list
	watchRequestFound := false
	for _, message := range messages {
		if _, ok := message.(*v1.WatchRequest); ok {
			watchRequestFound = true
		}
	}
	require.True(t, watchRequestFound, "did not find a CheckPermissionRequest in the list")
}
