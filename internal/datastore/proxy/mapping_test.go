package proxy

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/test"
	"github.com/authzed/spicedb/pkg/namespace"
)

const errIDNotFound = "unable to find mapping from id (%s) to namespace name"

type mappingTest struct {
	mapperMaker func() namespace.Mapper
}

type testAutoMapper struct {
	namespaceToID map[string]string
	idToNamespace map[string]string
}

func newID() string {
	id := uuid.NewString()
	for firstRune, _ := utf8.DecodeRuneInString(id); unicode.IsDigit(firstRune); firstRune, _ = utf8.DecodeRuneInString(id) {
		id = uuid.NewString()
	}
	return strings.ReplaceAll(id, "-", "")
}

func (tam testAutoMapper) Encode(name string) (string, error) {
	if found, ok := tam.namespaceToID[name]; ok {
		return found, nil
	}

	id := newID()
	tam.namespaceToID[name] = id
	tam.idToNamespace[id] = name

	return id, nil
}

func (tam testAutoMapper) Reverse(id string) (string, error) {
	if found, ok := tam.idToNamespace[id]; ok {
		return found, nil
	}

	return "", fmt.Errorf(errIDNotFound, id)
}

func (mt mappingTest) New(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	delegate, err := memdb.NewMemdbDatastore(watchBufferLength, revisionFuzzingTimedelta, gcWindow, 0)
	if err != nil {
		return nil, err
	}

	mapper := mt.mapperMaker()

	return NewMappingProxy(delegate, mapper, watchBufferLength), nil
}

func TestMappingDatastoreProxy(t *testing.T) {
	test.TestAll(t, mappingTest{func() namespace.Mapper {
		return testAutoMapper{
			make(map[string]string),
			make(map[string]string),
		}
	}})
}

func TestMappingDatastoreProxyPassthrough(t *testing.T) {
	test.TestAll(t, mappingTest{func() namespace.Mapper {
		return namespace.PassthroughMapper
	}})
}
