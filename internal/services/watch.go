package services

import api "github.com/authzed/spicedb/internal/REDACTEDapi/api"

type watchServer struct {
	api.UnimplementedWatchServiceServer
}

// NewWatchServer creates an instance of the watch server.
func NewWatchServer() api.WatchServiceServer {
	s := &watchServer{}
	return s
}
