package dispatch

import v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"

type MetadataError interface {
	error

	GetMetadata() *v1.ResponseMeta
}

type metadataErr struct {
	error
	metadata *v1.ResponseMeta
}

func (err metadataErr) Unwrap() error { return err.error }

func (err metadataErr) GetMetadata() *v1.ResponseMeta {
	return err.metadata
}

func WrapWithMetadata(baseErr error, metadata *v1.ResponseMeta) MetadataError {
	return &metadataErr{
		baseErr,
		metadata,
	}
}
