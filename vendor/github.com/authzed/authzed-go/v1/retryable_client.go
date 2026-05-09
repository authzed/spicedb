package authzed

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// ConflictStrategy is an enumeration type that represents the strategy to be used
// when a conflict occurs during a bulk import of relationships into SpiceDB.
type ConflictStrategy int

const (
	// Fail - The operation will fail if any duplicate relationships are found.
	Fail ConflictStrategy = iota
	// Skip - The operation will ignore duplicates and continue with the import.
	Skip
	// Touch - The operation will retry the import with TOUCH semantics in case of duplicates.
	Touch

	defaultBackoff    = 50 * time.Millisecond
	defaultMaxRetries = 10
)

// Fallback for datastore implementations on SpiceDB < 1.29.0 not returning proper gRPC codes
// Remove once https://github.com/authzed/spicedb/pull/1688 lands
var (
	txConflictCodes = []string{
		"SQLSTATE 23505",     // CockroachDB
		"Error 1062 (23000)", // MySQL
	}
	retryableErrorCodes = []string{
		"retryable error",                          // CockroachDB, PostgreSQL
		"try restarting transaction", "Error 1205", // MySQL
	}
)

// RetryableClient represents an open connection to SpiceDB with
// experimental services available. It also adds a new method for
// retrying bulk imports with different conflict strategies.
//
// Clients are backed by a gRPC client and as such are thread-safe.
type RetryableClient struct {
	ClientWithExperimental
}

// NewRetryableClient initializes a brand new client for interacting
// with SpiceDB.
func NewRetryableClient(endpoint string, opts ...grpc.DialOption) (*RetryableClient, error) {
	conn, err := newConn(endpoint, opts...)
	if err != nil {
		return nil, err
	}

	return &RetryableClient{
		ClientWithExperimental{
			Client{
				conn,
				v1.NewSchemaServiceClient(conn),
				v1.NewPermissionsServiceClient(conn),
				v1.NewWatchServiceClient(conn),
			},
			v1.NewExperimentalServiceClient(conn),
		},
	}, nil
}

// RetryableBulkImportRelationships is a wrapper around ImportBulkRelationships.
// It retries the bulk import with different conflict strategies in case of failure.
// The conflict strategy can be one of Fail, Skip, or Touch.
// Fail will return an error if any duplicate relationships are found.
// Skip will ignore duplicates and continue with the import.
// Touch will retry the import with TOUCH semantics in case of duplicates.
func (rc *RetryableClient) RetryableBulkImportRelationships(ctx context.Context, relationships []*v1.Relationship, conflictStrategy ConflictStrategy) error {
	bulkImportClient, err := rc.ImportBulkRelationships(ctx)
	if err != nil {
		return fmt.Errorf("error creating writer stream: %w", err)
	}

	// Error handled later during CloseAndRecv call
	_ = bulkImportClient.Send(&v1.ImportBulkRelationshipsRequest{
		Relationships: relationships,
	})

	_, err = bulkImportClient.CloseAndRecv() // transaction commit happens here
	if err == nil {
		return nil
	}

	// Failure to commit transaction means the stream is closed, so it can't be reused any further
	// The retry will be done using WriteRelationships instead of ImportBulkRelationships
	// This lets us retry with TOUCH semantics in case of failure due to duplicates
	retryable := isRetryableError(err)
	conflict := isAlreadyExistsError(err)
	canceled, cancelErr := isCanceledError(ctx.Err(), err)

	switch {
	case canceled:
		return cancelErr
	case conflict && conflictStrategy == Skip:
		return nil
	case retryable || (conflict && conflictStrategy == Touch):
		err = rc.writeBatchesWithRetry(ctx, relationships)
		if err != nil {
			return fmt.Errorf("failed to write relationships after retry: %w", err)
		}
		return nil
	case conflict && conflictStrategy == Fail:
		return fmt.Errorf("duplicate relationships found")
	default:
		return fmt.Errorf("error finalizing write of %d relationships: %w", len(relationships), err)
	}
}

func (rc *RetryableClient) writeBatchesWithRetry(ctx context.Context, relationships []*v1.Relationship) error {
	backoffInterval := backoff.NewExponentialBackOff()
	backoffInterval.InitialInterval = defaultBackoff
	backoffInterval.MaxInterval = 2 * time.Second
	backoffInterval.MaxElapsedTime = 0
	backoffInterval.Reset()

	currentRetries := 0

	updates := make([]*v1.RelationshipUpdate, len(relationships))
	for i, item := range relationships {
		updates[i] = &v1.RelationshipUpdate{
			Relationship: item,
			Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
		}
	}

	for {
		cancelCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := rc.WriteRelationships(cancelCtx, &v1.WriteRelationshipsRequest{Updates: updates})
		cancel()

		if isRetryableError(err) && currentRetries < defaultMaxRetries {
			// throttle the writes so we don't overwhelm the server
			bo := backoffInterval.NextBackOff()
			time.Sleep(bo)
			currentRetries++

			continue
		}
		if err != nil {
			return err
		}

		break
	}

	return nil
}

func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}

	if isGRPCCode(err, codes.AlreadyExists) {
		return true
	}

	return isContainsErrorString(err, txConflictCodes...)
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	if isGRPCCode(err, codes.Unavailable, codes.DeadlineExceeded) {
		return true
	}

	if isContainsErrorString(err, retryableErrorCodes...) {
		return true
	}

	return errors.Is(err, context.DeadlineExceeded)
}

func isCanceledError(errs ...error) (bool, error) {
	for _, err := range errs {
		if err == nil {
			continue
		}

		if errors.Is(err, context.Canceled) {
			return true, err
		}

		if isGRPCCode(err, codes.Canceled) {
			return true, err
		}
	}

	return false, nil
}

func isContainsErrorString(err error, errStrings ...string) bool {
	if err == nil {
		return false
	}

	for _, errString := range errStrings {
		if strings.Contains(err.Error(), errString) {
			return true
		}
	}

	return false
}

func isGRPCCode(err error, codes ...codes.Code) bool {
	if err == nil {
		return false
	}

	if s, ok := status.FromError(err); ok {
		for _, code := range codes {
			if s.Code() == code {
				return true
			}
		}
	}

	return false
}
