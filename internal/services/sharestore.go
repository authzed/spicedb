package services

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	sharedDataVersion = "1"
	maximumDataSize   = 1024 * 1024 * 1024 // 1 MB
	hashPrefixSize    = 12
)

// SharedData represents the data stored in a shared playground file.
type SharedData struct {
	Version          string   `json:"version"`
	NamespaceConfigs []string `json:"namespace_configs"`
	RelationTuples   string   `json:"relation_tuples"`
	ValidationYaml   string   `json:"validation_yaml"`
	AssertionsYaml   string   `json:"assertions_yaml"`
}

// ShareStore defines the interface for sharing and loading shared playground files.
type ShareStore interface {
	// LookupSharedByReference returns the shared data for the given reference hash, if any.
	LookupSharedByReference(reference string) (SharedData, bool, error)

	// StoreShared stores the given shared playground data in the backing storage, and returns
	// its reference hash.
	StoreShared(data SharedData) (string, error)
}

func newInMemoryShareStore() *inMemoryShareStore {
	return &inMemoryShareStore{
		shared: map[string]SharedData{},
		salt:   "flavored",
	}
}

type inMemoryShareStore struct {
	shared map[string]SharedData
	salt   string
}

func (ims *inMemoryShareStore) LookupSharedByReference(reference string) (SharedData, bool, error) {
	found, ok := ims.shared[reference]
	return found, ok, nil
}

func (ims *inMemoryShareStore) StoreShared(data SharedData) (string, error) {
	m, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	reference := computeShareHash(ims.salt, m)
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
func NewS3ShareStore(bucket string, salt string, config *aws.Config) (*s3ShareStore, error) {
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

func (s3s *s3ShareStore) LookupSharedByReference(reference string) (SharedData, bool, error) {
	key, err := s3s.key(reference)
	if err != nil {
		return SharedData{}, false, err
	}

	ctx := context.Background()

	result, err := s3s.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s3s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			return SharedData{}, false, nil
		}
		return SharedData{}, false, aerr
	}
	defer result.Body.Close()

	contentBytes, err := ioutil.ReadAll(result.Body)
	if err != nil {
		return SharedData{}, false, err
	}

	var sd SharedData
	err = json.Unmarshal(contentBytes, &sd)
	if err != nil {
		return SharedData{}, false, err
	}

	if sd.Version != sharedDataVersion {
		return sd, false, fmt.Errorf("unsupported version")
	}

	return sd, true, nil
}

func (s3s *s3ShareStore) StoreShared(data SharedData) (string, error) {
	m, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	reference := computeShareHash(s3s.salt, m)
	key, err := s3s.key(reference)
	if err != nil {
		return "", err
	}

	ctx := context.Background()

	_, err = s3s.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s3s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(m),
	})

	return reference, err
}

func computeShareHash(salt string, data []byte) string {
	h := sha256.New()

	io.WriteString(h, salt)
	io.WriteString(h, ":")
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
