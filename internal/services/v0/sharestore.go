package v0

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	sharedDataVersion = "2"
	hashPrefixSize    = 12
)

type versioned struct {
	Version string `json:"version"`
}

// SharedDataV1 represents the data stored in a shared playground file.
type SharedDataV1 struct {
	Version          string   `json:"version"`
	NamespaceConfigs []string `json:"namespace_configs"`
	RelationTuples   string   `json:"relation_tuples"`
	ValidationYaml   string   `json:"validation_yaml"`
	AssertionsYaml   string   `json:"assertions_yaml"`
}

// SharedDataV2 represents the data stored in a shared playground file.
type SharedDataV2 struct {
	Version           string `json:"version" yaml:"-"`
	Schema            string `json:"schema" yaml:"schema"`
	RelationshipsYaml string `json:"relationships_yaml" yaml:"relationships"`
	ValidationYaml    string `json:"validation_yaml" yaml:"validation"`
	AssertionsYaml    string `json:"assertions_yaml" yaml:"assertions"`
}

// LookupStatus is an enum for the possible ShareStore lookup outcomes.
type LookupStatus int

const (
	// LookupError indicates an error has occurred.
	LookupError LookupStatus = iota

	// LookupNotFound indicates that no results were found for the specified reference.
	LookupNotFound

	// LookupSuccess indicates success.
	LookupSuccess

	// LookupConverted indicates when the results have been converted from an earlier version.
	LookupConverted
)

// ShareStore defines the interface for sharing and loading shared playground files.
type ShareStore interface {
	// LookupSharedByReference returns the shared data for the given reference hash, if any.
	LookupSharedByReference(reference string) (SharedDataV2, LookupStatus, error)

	// StoreShared stores the given shared playground data in the backing storage, and returns
	// its reference hash.
	StoreShared(data SharedDataV2) (string, error)
}

// NewInMemoryShareStore creates a new in memory share store.
func NewInMemoryShareStore(salt string) ShareStore {
	return &inMemoryShareStore{
		shared: map[string][]byte{},
		salt:   salt,
	}
}

type inMemoryShareStore struct {
	shared map[string][]byte
	salt   string
}

func (ims *inMemoryShareStore) LookupSharedByReference(reference string) (SharedDataV2, LookupStatus, error) {
	found, ok := ims.shared[reference]
	if !ok {
		return SharedDataV2{}, LookupNotFound, nil
	}

	return unmarshalShared(found)
}

func (ims *inMemoryShareStore) StoreShared(shared SharedDataV2) (string, error) {
	data, reference, err := marshalShared(shared, ims.salt)
	if err != nil {
		return "", err
	}

	ims.shared[reference] = data
	return reference, nil
}

type s3ShareStore struct {
	bucket   string
	salt     string
	s3Client *s3.S3
}

// NewS3ShareStore creates a new S3 share store, reading and writing the shared data to the given
// bucket, with the given salt for hash computation and the given config for connecting to S3 or
// and S3-compatible API.
func NewS3ShareStore(bucket string, salt string, config *aws.Config) (ShareStore, error) {
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}

	// Create S3 service client
	s3Client := s3.New(sess)
	return &s3ShareStore{
		salt:     salt,
		s3Client: s3Client,
		bucket:   bucket,
	}, nil
}

func (s3s *s3ShareStore) createBucketForTesting() error {
	cparams := &s3.CreateBucketInput{
		Bucket: aws.String(s3s.bucket),
	}

	_, err := s3s.s3Client.CreateBucket(cparams)
	return err
}

// NOTE: Copied from base64.go for URL-encoding, since it isn't exported.
const encodeURL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

func (s3s *s3ShareStore) key(reference string) (string, error) {
	// Ensure it is a local safe identifier.
	for _, r := range reference {
		if !strings.ContainsRune(encodeURL, r) {
			return "", fmt.Errorf("invalid reference")
		}
	}

	return "shared/" + reference, nil
}

func (s3s *s3ShareStore) LookupSharedByReference(reference string) (SharedDataV2, LookupStatus, error) {
	key, err := s3s.key(reference)
	if err != nil {
		return SharedDataV2{}, LookupError, err
	}

	ctx := context.Background()

	result, err := s3s.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s3s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var aerr awserr.Error
		if errors.As(err, &aerr) && aerr.Code() == s3.ErrCodeNoSuchKey {
			return SharedDataV2{}, LookupNotFound, nil
		}
		return SharedDataV2{}, LookupError, aerr
	}
	defer result.Body.Close()

	contentBytes, err := io.ReadAll(result.Body)
	if err != nil {
		return SharedDataV2{}, LookupError, err
	}

	return unmarshalShared(contentBytes)
}

func (s3s *s3ShareStore) StoreShared(shared SharedDataV2) (string, error) {
	data, reference, err := marshalShared(shared, s3s.salt)
	if err != nil {
		return "", err
	}

	key, err := s3s.key(reference)
	if err != nil {
		return "", err
	}

	ctx := context.Background()

	_, err = s3s.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s3s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})

	return reference, err
}

func computeShareHash(salt string, data []byte) string {
	h := sha256.New()
	_, _ = io.WriteString(h, salt+":")
	h.Write(data)

	sum := h.Sum(nil)
	b := make([]byte, base64.URLEncoding.EncodedLen(len(sum)))
	base64.URLEncoding.Encode(b, sum)

	// NOTE: According to https://github.com/golang/playground/blob/48a1655aa6e55ac2658d07abcb3b39d61784f035/share.go#L39,
	// some systems don't like URLs which end in underscores, so increase the hash length until
	// we don't end in one.
	hashLen := hashPrefixSize
	for hashLen <= len(b) && b[hashLen-1] == '_' {
		hashLen++
	}
	return string(b)[:hashLen]
}

func marshalShared(shared SharedDataV2, salt string) ([]byte, string, error) {
	marshalled, err := json.Marshal(shared)
	if err != nil {
		return []byte{}, "", err
	}

	return marshalled, computeShareHash(salt, marshalled), nil
}

func unmarshalShared(data []byte) (SharedDataV2, LookupStatus, error) {
	var v versioned
	err := json.Unmarshal(data, &v)
	if err != nil {
		return SharedDataV2{}, LookupError, err
	}

	switch v.Version {
	case "2":
		var v2 SharedDataV2
		err = json.Unmarshal(data, &v2)
		if err != nil {
			return SharedDataV2{}, LookupError, err
		}
		return v2, LookupSuccess, nil

	case "1":
		var v1 SharedDataV1
		err = json.Unmarshal(data, &v1)
		if err != nil {
			return SharedDataV2{}, LookupError, err
		}

		// Convert to a V2 data structure.
		upgraded, err := upgradeSchema(v1.NamespaceConfigs)
		if err != nil {
			return SharedDataV2{}, LookupError, err
		}

		return SharedDataV2{
			Schema:            upgraded,
			RelationshipsYaml: v1.RelationTuples,
			ValidationYaml:    v1.ValidationYaml,
			AssertionsYaml:    v1.AssertionsYaml,
		}, LookupConverted, nil

	default:
		return SharedDataV2{}, LookupError, fmt.Errorf("unsupported share version %s", v.Version)
	}
}
