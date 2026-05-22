package datalayer

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// --- fake helpers ---

// fakeRevision is a simple datastore.Revision for unit tests.
type fakeRevision string

func (r fakeRevision) String() string                      { return string(r) }
func (r fakeRevision) Equal(o datastore.Revision) bool     { return r.String() == o.String() }
func (r fakeRevision) GreaterThan(datastore.Revision) bool { return false }
func (r fakeRevision) LessThan(datastore.Revision) bool    { return false }
func (r fakeRevision) ByteSortable() bool                  { return false }

var testRevision fakeRevision = "rev1"

// fakeLegacySchemaReader is a fake datastore.LegacySchemaReader with canned responses.
type fakeLegacySchemaReader struct {
	namespaces      []datastore.RevisionedNamespace
	caveats         []datastore.RevisionedCaveat
	namespacesErr   error
	caveatsErr      error
	readNSByName    map[string]*core.NamespaceDefinition
	readNSRevision  datastore.Revision
	readNSErr       error
	readCavByName   map[string]*core.CaveatDefinition
	readCavRev      datastore.Revision
	readCavErr      error
	lookupNSResult  []datastore.RevisionedNamespace
	lookupNSErr     error
	lookupCavResult []datastore.RevisionedCaveat
	lookupCavErr    error
}

func (m *fakeLegacySchemaReader) LegacyReadNamespaceByName(_ context.Context, name string) (*core.NamespaceDefinition, datastore.Revision, error) {
	if m.readNSErr != nil {
		return nil, nil, m.readNSErr
	}
	ns, ok := m.readNSByName[name]
	if !ok {
		return nil, nil, datastore.NewNamespaceNotFoundErr(name)
	}
	return ns, m.readNSRevision, nil
}

func (m *fakeLegacySchemaReader) LegacyListAllNamespaces(_ context.Context) ([]datastore.RevisionedNamespace, error) {
	return m.namespaces, m.namespacesErr
}

func (m *fakeLegacySchemaReader) LegacyLookupNamespacesWithNames(_ context.Context, _ []string) ([]datastore.RevisionedNamespace, error) {
	return m.lookupNSResult, m.lookupNSErr
}

func (m *fakeLegacySchemaReader) LegacyReadCaveatByName(_ context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	if m.readCavErr != nil {
		return nil, nil, m.readCavErr
	}
	cav, ok := m.readCavByName[name]
	if !ok {
		return nil, nil, datastore.NewCaveatNameNotFoundErr(name)
	}
	return cav, m.readCavRev, nil
}

func (m *fakeLegacySchemaReader) LegacyListAllCaveats(_ context.Context) ([]datastore.RevisionedCaveat, error) {
	return m.caveats, m.caveatsErr
}

func (m *fakeLegacySchemaReader) LegacyLookupCaveatsWithNames(_ context.Context, _ []string) ([]datastore.RevisionedCaveat, error) {
	return m.lookupCavResult, m.lookupCavErr
}

// fakeLegacySchemaWriter is a fake datastore.LegacySchemaWriter that records calls.
type fakeLegacySchemaWriter struct {
	fakeLegacySchemaReader

	writtenNS      []*core.NamespaceDefinition
	writtenCaveats []*core.CaveatDefinition
	deletedNS      []string
	deletedCaveats []string
	writeNSErr     error
	writeCavErr    error
	deleteNSErr    error
	deleteCavErr   error
}

func (m *fakeLegacySchemaWriter) LegacyWriteNamespaces(_ context.Context, nsDefs ...*core.NamespaceDefinition) error {
	m.writtenNS = append(m.writtenNS, nsDefs...)
	return m.writeNSErr
}

func (m *fakeLegacySchemaWriter) LegacyWriteCaveats(_ context.Context, cavDefs []*core.CaveatDefinition) error {
	m.writtenCaveats = append(m.writtenCaveats, cavDefs...)
	return m.writeCavErr
}

func (m *fakeLegacySchemaWriter) LegacyDeleteNamespaces(_ context.Context, names []string, _ datastore.DeleteNamespacesRelationshipsOption) error {
	m.deletedNS = append(m.deletedNS, names...)
	return m.deleteNSErr
}

func (m *fakeLegacySchemaWriter) LegacyDeleteCaveats(_ context.Context, names []string) error {
	m.deletedCaveats = append(m.deletedCaveats, names...)
	return m.deleteCavErr
}

// --- legacySchemaReaderAdapter tests ---

func TestLegacyAdapter_SchemaText_Empty(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{}
	adapter := SchemaReaderFromLegacy(reader)

	_, err := adapter.SchemaText(t.Context())
	require.Error(t, err)
}

func TestLegacyAdapter_SchemaText_ListNamespacesError(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{namespacesErr: errors.New("boom")}
	adapter := SchemaReaderFromLegacy(reader)

	_, err := adapter.SchemaText(t.Context())
	require.ErrorContains(t, err, "boom")
}

func TestLegacyAdapter_SchemaText_ListCaveatsError(t *testing.T) {
	t.Parallel()
	ns := &core.NamespaceDefinition{Name: "user"}
	reader := &fakeLegacySchemaReader{
		namespaces: []datastore.RevisionedNamespace{
			{Definition: ns, LastWrittenRevision: testRevision},
		},
		caveatsErr: errors.New("caveat err"),
	}
	adapter := SchemaReaderFromLegacy(reader)

	_, err := adapter.SchemaText(t.Context())
	require.ErrorContains(t, err, "caveat err")
}

func TestLegacyAdapter_SchemaText_WithDefinitions(t *testing.T) {
	t.Parallel()
	ns := &core.NamespaceDefinition{Name: "user"}
	reader := &fakeLegacySchemaReader{
		namespaces: []datastore.RevisionedNamespace{
			{Definition: ns, LastWrittenRevision: testRevision},
		},
	}
	adapter := SchemaReaderFromLegacy(reader)

	text, err := adapter.SchemaText(t.Context())
	require.NoError(t, err)
	require.Contains(t, text, "user")
}

func TestLegacyAdapter_LookupTypeDefByName_Found(t *testing.T) {
	t.Parallel()
	ns := &core.NamespaceDefinition{Name: "document"}
	reader := &fakeLegacySchemaReader{
		readNSByName:   map[string]*core.NamespaceDefinition{"document": ns},
		readNSRevision: testRevision,
	}
	adapter := SchemaReaderFromLegacy(reader)

	result, found, err := adapter.LookupTypeDefByName(t.Context(), "document")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "document", result.Definition.Name)
	require.Equal(t, testRevision, result.LastWrittenRevision)
}

func TestLegacyAdapter_LookupTypeDefByName_NotFound(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{
		readNSByName: map[string]*core.NamespaceDefinition{},
	}
	adapter := SchemaReaderFromLegacy(reader)

	_, found, err := adapter.LookupTypeDefByName(t.Context(), "missing")
	require.NoError(t, err)
	require.False(t, found)
}

func TestLegacyAdapter_LookupTypeDefByName_OtherError(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{
		readNSErr: errors.New("db down"),
	}
	adapter := SchemaReaderFromLegacy(reader)

	_, _, err := adapter.LookupTypeDefByName(t.Context(), "anything")
	require.ErrorContains(t, err, "db down")
}

func TestLegacyAdapter_LookupCaveatDefByName_Found(t *testing.T) {
	t.Parallel()
	cav := &core.CaveatDefinition{Name: "mycaveat"}
	reader := &fakeLegacySchemaReader{
		readCavByName: map[string]*core.CaveatDefinition{"mycaveat": cav},
		readCavRev:    testRevision,
	}
	adapter := SchemaReaderFromLegacy(reader)

	result, found, err := adapter.LookupCaveatDefByName(t.Context(), "mycaveat")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "mycaveat", result.Definition.Name)
	require.Equal(t, testRevision, result.LastWrittenRevision)
}

func TestLegacyAdapter_LookupCaveatDefByName_NotFound(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{
		readCavByName: map[string]*core.CaveatDefinition{},
	}
	adapter := SchemaReaderFromLegacy(reader)

	_, found, err := adapter.LookupCaveatDefByName(t.Context(), "missing")
	require.NoError(t, err)
	require.False(t, found)
}

func TestLegacyAdapter_LookupCaveatDefByName_OtherError(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{
		readCavErr: errors.New("db error"),
	}
	adapter := SchemaReaderFromLegacy(reader)

	_, _, err := adapter.LookupCaveatDefByName(t.Context(), "anything")
	require.ErrorContains(t, err, "db error")
}

func TestLegacyAdapter_ListAllTypeDefinitions(t *testing.T) {
	t.Parallel()
	nsDefs := []datastore.RevisionedNamespace{
		{Definition: &core.NamespaceDefinition{Name: "user"}, LastWrittenRevision: testRevision},
		{Definition: &core.NamespaceDefinition{Name: "doc"}, LastWrittenRevision: testRevision},
	}
	reader := &fakeLegacySchemaReader{namespaces: nsDefs}
	adapter := SchemaReaderFromLegacy(reader)

	result, err := adapter.ListAllTypeDefinitions(t.Context())
	require.NoError(t, err)
	require.Len(t, result, 2)
}

func TestLegacyAdapter_ListAllTypeDefinitions_Error(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{namespacesErr: errors.New("fail")}
	adapter := SchemaReaderFromLegacy(reader)

	_, err := adapter.ListAllTypeDefinitions(t.Context())
	require.ErrorContains(t, err, "fail")
}

func TestLegacyAdapter_ListAllCaveatDefinitions(t *testing.T) {
	t.Parallel()
	cavDefs := []datastore.RevisionedCaveat{
		{Definition: &core.CaveatDefinition{Name: "cav1"}, LastWrittenRevision: testRevision},
	}
	reader := &fakeLegacySchemaReader{caveats: cavDefs}
	adapter := SchemaReaderFromLegacy(reader)

	result, err := adapter.ListAllCaveatDefinitions(t.Context())
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "cav1", result[0].Definition.Name)
}

func TestLegacyAdapter_ListAllCaveatDefinitions_Error(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{caveatsErr: errors.New("fail")}
	adapter := SchemaReaderFromLegacy(reader)

	_, err := adapter.ListAllCaveatDefinitions(t.Context())
	require.ErrorContains(t, err, "fail")
}

func TestLegacyAdapter_ListAllSchemaDefinitions(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{
		namespaces: []datastore.RevisionedNamespace{
			{Definition: &core.NamespaceDefinition{Name: "user"}, LastWrittenRevision: testRevision},
		},
		caveats: []datastore.RevisionedCaveat{
			{Definition: &core.CaveatDefinition{Name: "cav1"}, LastWrittenRevision: testRevision},
		},
	}
	adapter := SchemaReaderFromLegacy(reader)

	result, err := adapter.ListAllSchemaDefinitions(t.Context())
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Contains(t, result, "user")
	require.Contains(t, result, "cav1")
}

func TestLegacyAdapter_ListAllSchemaDefinitions_NSError(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{namespacesErr: errors.New("ns fail")}
	adapter := SchemaReaderFromLegacy(reader)

	_, err := adapter.ListAllSchemaDefinitions(t.Context())
	require.ErrorContains(t, err, "ns fail")
}

func TestLegacyAdapter_ListAllSchemaDefinitions_CaveatError(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{
		namespaces: []datastore.RevisionedNamespace{
			{Definition: &core.NamespaceDefinition{Name: "user"}, LastWrittenRevision: testRevision},
		},
		caveatsErr: errors.New("cav fail"),
	}
	adapter := SchemaReaderFromLegacy(reader)

	_, err := adapter.ListAllSchemaDefinitions(t.Context())
	require.ErrorContains(t, err, "cav fail")
}

func TestLegacyAdapter_LookupSchemaDefinitionsByNames(t *testing.T) {
	t.Parallel()
	ns := &core.NamespaceDefinition{Name: "user"}
	cav := &core.CaveatDefinition{Name: "mycav"}
	reader := &fakeLegacySchemaReader{
		lookupNSResult: []datastore.RevisionedNamespace{
			{Definition: ns, LastWrittenRevision: testRevision},
		},
		lookupCavResult: []datastore.RevisionedCaveat{
			{Definition: cav, LastWrittenRevision: testRevision},
		},
	}
	adapter := SchemaReaderFromLegacy(reader)

	result, err := adapter.LookupSchemaDefinitionsByNames(t.Context(), []string{"user", "mycav"})
	require.NoError(t, err)
	require.Contains(t, result, "user")
	require.Contains(t, result, "mycav")
}

func TestLegacyAdapter_LookupSchemaDefinitionsByNames_NSError(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{lookupNSErr: errors.New("ns lookup fail")}
	adapter := SchemaReaderFromLegacy(reader)

	_, err := adapter.LookupSchemaDefinitionsByNames(t.Context(), []string{"foo"})
	require.ErrorContains(t, err, "ns lookup fail")
}

func TestLegacyAdapter_LookupSchemaDefinitionsByNames_CaveatError(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{
		lookupNSResult: []datastore.RevisionedNamespace{},
		lookupCavErr:   errors.New("cav lookup fail"),
	}
	adapter := SchemaReaderFromLegacy(reader)

	_, err := adapter.LookupSchemaDefinitionsByNames(t.Context(), []string{"foo"})
	require.ErrorContains(t, err, "cav lookup fail")
}

func TestLegacyAdapter_LookupTypeDefinitionsByNames(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{
		lookupNSResult: []datastore.RevisionedNamespace{
			{Definition: &core.NamespaceDefinition{Name: "user"}, LastWrittenRevision: testRevision},
			{Definition: &core.NamespaceDefinition{Name: "doc"}, LastWrittenRevision: testRevision},
		},
	}
	adapter := SchemaReaderFromLegacy(reader)

	result, err := adapter.LookupTypeDefinitionsByNames(t.Context(), []string{"user", "doc"})
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Contains(t, result, "user")
	require.Contains(t, result, "doc")
}

func TestLegacyAdapter_LookupTypeDefinitionsByNames_Error(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{lookupNSErr: errors.New("err")}
	adapter := SchemaReaderFromLegacy(reader)

	_, err := adapter.LookupTypeDefinitionsByNames(t.Context(), []string{"user"})
	require.ErrorContains(t, err, "err")
}

func TestLegacyAdapter_LookupCaveatDefinitionsByNames(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{
		lookupCavResult: []datastore.RevisionedCaveat{
			{Definition: &core.CaveatDefinition{Name: "c1"}, LastWrittenRevision: testRevision},
		},
	}
	adapter := SchemaReaderFromLegacy(reader)

	result, err := adapter.LookupCaveatDefinitionsByNames(t.Context(), []string{"c1", "missing"})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Contains(t, result, "c1")
}

func TestLegacyAdapter_LookupCaveatDefinitionsByNames_Error(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{lookupCavErr: errors.New("err")}
	adapter := SchemaReaderFromLegacy(reader)

	_, err := adapter.LookupCaveatDefinitionsByNames(t.Context(), []string{"c1"})
	require.ErrorContains(t, err, "err")
}

// --- storedSchemaReaderAdapter tests ---

func newStoredAdapter(nsDefs map[string]*core.NamespaceDefinition, cavDefs map[string]*core.CaveatDefinition, schemaText string) *storedSchemaReaderAdapter {
	return &storedSchemaReaderAdapter{
		storedSchema: datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
			Version: 1,
			VersionOneof: &core.StoredSchema_V1{
				V1: &core.StoredSchema_V1StoredSchema{
					SchemaText:           schemaText,
					NamespaceDefinitions: nsDefs,
					CaveatDefinitions:    cavDefs,
				},
			},
		}),
		lastWrittenRevision: testRevision,
	}
}

func TestStoredAdapter_SchemaText(t *testing.T) {
	t.Parallel()
	adapter := newStoredAdapter(
		map[string]*core.NamespaceDefinition{"user": {Name: "user"}},
		nil,
		"definition user {}",
	)

	text, err := adapter.SchemaText(t.Context())
	require.NoError(t, err)
	require.Equal(t, "definition user {}", text)
}

func TestStoredAdapter_SchemaText_EmptyWithDefinitions(t *testing.T) {
	t.Parallel()
	// SchemaText is empty string but definitions exist -> return empty string, no error.
	adapter := newStoredAdapter(
		map[string]*core.NamespaceDefinition{"user": {Name: "user"}},
		nil,
		"",
	)

	text, err := adapter.SchemaText(t.Context())
	require.NoError(t, err)
	require.Empty(t, text)
}

func TestStoredAdapter_SchemaText_EmptyNoDefinitions(t *testing.T) {
	t.Parallel()
	adapter := newStoredAdapter(nil, nil, "")

	_, err := adapter.SchemaText(t.Context())
	require.Error(t, err)
}

func TestStoredAdapter_LookupTypeDefByName_Found(t *testing.T) {
	t.Parallel()
	ns := &core.NamespaceDefinition{Name: "doc"}
	adapter := newStoredAdapter(map[string]*core.NamespaceDefinition{"doc": ns}, nil, "")

	result, found, err := adapter.LookupTypeDefByName(t.Context(), "doc")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "doc", result.Definition.Name)
	require.Equal(t, testRevision, result.LastWrittenRevision)
}

func TestStoredAdapter_LookupTypeDefByName_NotFound(t *testing.T) {
	t.Parallel()
	adapter := newStoredAdapter(nil, nil, "")

	_, found, err := adapter.LookupTypeDefByName(t.Context(), "missing")
	require.NoError(t, err)
	require.False(t, found)
}

func TestStoredAdapter_LookupCaveatDefByName_Found(t *testing.T) {
	t.Parallel()
	cav := &core.CaveatDefinition{Name: "c1"}
	adapter := newStoredAdapter(nil, map[string]*core.CaveatDefinition{"c1": cav}, "")

	result, found, err := adapter.LookupCaveatDefByName(t.Context(), "c1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "c1", result.Definition.Name)
	require.Equal(t, testRevision, result.LastWrittenRevision)
}

func TestStoredAdapter_LookupCaveatDefByName_NotFound(t *testing.T) {
	t.Parallel()
	adapter := newStoredAdapter(nil, nil, "")

	_, found, err := adapter.LookupCaveatDefByName(t.Context(), "missing")
	require.NoError(t, err)
	require.False(t, found)
}

func TestStoredAdapter_ListAllTypeDefinitions(t *testing.T) {
	t.Parallel()
	adapter := newStoredAdapter(
		map[string]*core.NamespaceDefinition{
			"user": {Name: "user"},
			"doc":  {Name: "doc"},
		}, nil, "")

	result, err := adapter.ListAllTypeDefinitions(t.Context())
	require.NoError(t, err)
	require.Len(t, result, 2)
	for _, td := range result {
		require.Equal(t, testRevision, td.LastWrittenRevision)
	}
}

func TestStoredAdapter_ListAllCaveatDefinitions(t *testing.T) {
	t.Parallel()
	adapter := newStoredAdapter(nil,
		map[string]*core.CaveatDefinition{
			"c1": {Name: "c1"},
		}, "")

	result, err := adapter.ListAllCaveatDefinitions(t.Context())
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "c1", result[0].Definition.Name)
	require.Equal(t, testRevision, result[0].LastWrittenRevision)
}

func TestStoredAdapter_ListAllSchemaDefinitions(t *testing.T) {
	t.Parallel()
	adapter := newStoredAdapter(
		map[string]*core.NamespaceDefinition{"user": {Name: "user"}},
		map[string]*core.CaveatDefinition{"c1": {Name: "c1"}},
		"",
	)

	result, err := adapter.ListAllSchemaDefinitions(t.Context())
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Contains(t, result, "user")
	require.Contains(t, result, "c1")
}

func TestStoredAdapter_LookupSchemaDefinitionsByNames(t *testing.T) {
	t.Parallel()
	adapter := newStoredAdapter(
		map[string]*core.NamespaceDefinition{"user": {Name: "user"}},
		map[string]*core.CaveatDefinition{"c1": {Name: "c1"}},
		"",
	)

	result, err := adapter.LookupSchemaDefinitionsByNames(t.Context(), []string{"user", "c1", "missing"})
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Contains(t, result, "user")
	require.Contains(t, result, "c1")
}

func TestStoredAdapter_LookupSchemaDefinitionsByNames_NamespacePreferredOverCaveat(t *testing.T) {
	t.Parallel()
	// When both a namespace and caveat have the same name, the namespace should be found first.
	adapter := newStoredAdapter(
		map[string]*core.NamespaceDefinition{"shared": {Name: "shared"}},
		map[string]*core.CaveatDefinition{"shared": {Name: "shared"}},
		"",
	)

	result, err := adapter.LookupSchemaDefinitionsByNames(t.Context(), []string{"shared"})
	require.NoError(t, err)
	require.Len(t, result, 1)
	// The namespace def should be returned since the code checks namespaces first.
	_, isNS := result["shared"].(*core.NamespaceDefinition)
	require.True(t, isNS)
}

func TestStoredAdapter_LookupTypeDefinitionsByNames(t *testing.T) {
	t.Parallel()
	adapter := newStoredAdapter(
		map[string]*core.NamespaceDefinition{
			"user": {Name: "user"},
			"doc":  {Name: "doc"},
		},
		map[string]*core.CaveatDefinition{"c1": {Name: "c1"}}, // should be ignored
		"",
	)

	result, err := adapter.LookupTypeDefinitionsByNames(t.Context(), []string{"user", "c1", "missing"})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Contains(t, result, "user")
}

func TestStoredAdapter_LookupCaveatDefinitionsByNames(t *testing.T) {
	t.Parallel()
	adapter := newStoredAdapter(
		map[string]*core.NamespaceDefinition{"user": {Name: "user"}}, // should be ignored
		map[string]*core.CaveatDefinition{"c1": {Name: "c1"}, "c2": {Name: "c2"}},
		"",
	)

	result, err := adapter.LookupCaveatDefinitionsByNames(t.Context(), []string{"c1", "user", "missing"})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Contains(t, result, "c1")
}

func TestStoredAdapter_V1NilFallback(t *testing.T) {
	t.Parallel()
	// When VersionOneof is nil, v1() should return an empty struct without panicking.
	adapter := &storedSchemaReaderAdapter{
		storedSchema:        datastore.NewReadOnlyStoredSchema(&core.StoredSchema{Version: 1}),
		lastWrittenRevision: testRevision,
	}

	_, err := adapter.SchemaText(t.Context())
	require.Error(t, err)

	defs, err := adapter.ListAllTypeDefinitions(t.Context())
	require.NoError(t, err)
	require.Empty(t, defs)
}

// --- newStoredSchemaReaderAdapter tests ---

type fakeStoredSchemaReader struct {
	schema *datastore.ReadOnlyStoredSchema
	err    error
}

func (m *fakeStoredSchemaReader) ReadStoredSchema(_ context.Context) (*datastore.ReadOnlyStoredSchema, error) {
	return m.schema, m.err
}

func TestNewStoredSchemaReaderAdapter_Success(t *testing.T) {
	t.Parallel()
	schema := datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
		Version: 1,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText: "definition user {}",
				NamespaceDefinitions: map[string]*core.NamespaceDefinition{
					"user": {Name: "user"},
				},
			},
		},
	})

	reader := &fakeStoredSchemaReader{schema: schema}
	adapter, err := newStoredSchemaReaderAdapter(t.Context(), reader, "somehash", testRevision, noopSchemaCache{})
	require.NoError(t, err)

	text, err := adapter.SchemaText(t.Context())
	require.NoError(t, err)
	require.Equal(t, "definition user {}", text)
}

func TestNewStoredSchemaReaderAdapter_SchemaNotFound(t *testing.T) {
	t.Parallel()
	reader := &fakeStoredSchemaReader{err: datastore.ErrSchemaNotFound}
	adapter, err := newStoredSchemaReaderAdapter(t.Context(), reader, "somehash", testRevision, noopSchemaCache{})
	require.NoError(t, err)

	// Should return empty adapter
	defs, err := adapter.ListAllTypeDefinitions(t.Context())
	require.NoError(t, err)
	require.Empty(t, defs)

	_, err = adapter.SchemaText(t.Context())
	require.Error(t, err) // SchemaNotDefinedErr
}

func TestNewStoredSchemaReaderAdapter_OtherError(t *testing.T) {
	t.Parallel()
	reader := &fakeStoredSchemaReader{err: errors.New("connection refused")}
	_, err := newStoredSchemaReaderAdapter(t.Context(), reader, "somehash", testRevision, noopSchemaCache{})
	require.ErrorContains(t, err, "connection refused")
}

// --- writeSchemaViaLegacy tests ---

func TestWriteSchemaViaLegacy_NewDefinitions(t *testing.T) {
	t.Parallel()
	writer := &fakeLegacySchemaWriter{}

	ns := &core.NamespaceDefinition{Name: "user"}
	cav := &core.CaveatDefinition{Name: "mycav"}
	defs := []datastore.SchemaDefinition{ns, cav}

	err := writeSchemaViaLegacy(t.Context(), writer, &writer.fakeLegacySchemaReader, defs)
	require.NoError(t, err)

	require.Len(t, writer.writtenNS, 1)
	require.Equal(t, "user", writer.writtenNS[0].Name)
	require.Len(t, writer.writtenCaveats, 1)
	require.Equal(t, "mycav", writer.writtenCaveats[0].Name)
}

func TestWriteSchemaViaLegacy_SkipsUnchanged(t *testing.T) {
	t.Parallel()
	ns := &core.NamespaceDefinition{Name: "user"}
	writer := &fakeLegacySchemaWriter{
		fakeLegacySchemaReader: fakeLegacySchemaReader{
			namespaces: []datastore.RevisionedNamespace{
				{Definition: ns, LastWrittenRevision: testRevision},
			},
		},
	}

	// Write the same namespace — should skip it since it's unchanged.
	err := writeSchemaViaLegacy(t.Context(), writer, &writer.fakeLegacySchemaReader, []datastore.SchemaDefinition{ns})
	require.NoError(t, err)
	require.Empty(t, writer.writtenNS)
}

func TestWriteSchemaViaLegacy_WritesChanged(t *testing.T) {
	t.Parallel()
	existingNS := &core.NamespaceDefinition{Name: "user"}
	updatedNS := &core.NamespaceDefinition{Name: "user", Metadata: &core.Metadata{}}
	writer := &fakeLegacySchemaWriter{
		fakeLegacySchemaReader: fakeLegacySchemaReader{
			namespaces: []datastore.RevisionedNamespace{
				{Definition: existingNS, LastWrittenRevision: testRevision},
			},
		},
	}

	err := writeSchemaViaLegacy(t.Context(), writer, &writer.fakeLegacySchemaReader, []datastore.SchemaDefinition{updatedNS})
	require.NoError(t, err)
	require.Len(t, writer.writtenNS, 1)
	require.Equal(t, "user", writer.writtenNS[0].Name)
}

func TestWriteSchemaViaLegacy_DeletesRemoved(t *testing.T) {
	t.Parallel()
	existingNS := &core.NamespaceDefinition{Name: "oldns"}
	existingCav := &core.CaveatDefinition{Name: "oldcav"}
	writer := &fakeLegacySchemaWriter{
		fakeLegacySchemaReader: fakeLegacySchemaReader{
			namespaces: []datastore.RevisionedNamespace{
				{Definition: existingNS, LastWrittenRevision: testRevision},
			},
			caveats: []datastore.RevisionedCaveat{
				{Definition: existingCav, LastWrittenRevision: testRevision},
			},
		},
	}

	// Write with no definitions -> should delete both.
	err := writeSchemaViaLegacy(t.Context(), writer, &writer.fakeLegacySchemaReader, nil)
	require.NoError(t, err)
	require.Contains(t, writer.deletedNS, "oldns")
	require.Contains(t, writer.deletedCaveats, "oldcav")
}

func TestWriteSchemaViaLegacy_DuplicateNameError(t *testing.T) {
	t.Parallel()
	writer := &fakeLegacySchemaWriter{}

	ns1 := &core.NamespaceDefinition{Name: "dup"}
	ns2 := &core.NamespaceDefinition{Name: "dup"}
	err := writeSchemaViaLegacy(t.Context(), writer, &writer.fakeLegacySchemaReader, []datastore.SchemaDefinition{ns1, ns2})
	require.ErrorContains(t, err, "duplicate definition name: dup")
}

func TestWriteSchemaViaLegacy_ListNamespacesError(t *testing.T) {
	t.Parallel()
	writer := &fakeLegacySchemaWriter{
		fakeLegacySchemaReader: fakeLegacySchemaReader{
			namespacesErr: errors.New("list ns fail"),
		},
	}

	ns := &core.NamespaceDefinition{Name: "user"}
	err := writeSchemaViaLegacy(t.Context(), writer, &writer.fakeLegacySchemaReader, []datastore.SchemaDefinition{ns})
	require.ErrorContains(t, err, "list ns fail")
}

func TestWriteSchemaViaLegacy_ListCaveatsError(t *testing.T) {
	t.Parallel()
	writer := &fakeLegacySchemaWriter{
		fakeLegacySchemaReader: fakeLegacySchemaReader{
			caveatsErr: errors.New("list cav fail"),
		},
	}

	ns := &core.NamespaceDefinition{Name: "user"}
	err := writeSchemaViaLegacy(t.Context(), writer, &writer.fakeLegacySchemaReader, []datastore.SchemaDefinition{ns})
	require.ErrorContains(t, err, "list cav fail")
}

func TestWriteSchemaViaLegacy_WriteNamespacesError(t *testing.T) {
	t.Parallel()
	writer := &fakeLegacySchemaWriter{writeNSErr: errors.New("write ns fail")}

	ns := &core.NamespaceDefinition{Name: "user"}
	err := writeSchemaViaLegacy(t.Context(), writer, &writer.fakeLegacySchemaReader, []datastore.SchemaDefinition{ns})
	require.ErrorContains(t, err, "write ns fail")
}

func TestWriteSchemaViaLegacy_WriteCaveatsError(t *testing.T) {
	t.Parallel()
	writer := &fakeLegacySchemaWriter{writeCavErr: errors.New("write cav fail")}

	cav := &core.CaveatDefinition{Name: "mycav"}
	err := writeSchemaViaLegacy(t.Context(), writer, &writer.fakeLegacySchemaReader, []datastore.SchemaDefinition{cav})
	require.ErrorContains(t, err, "write cav fail")
}

func TestWriteSchemaViaLegacy_DeleteNamespacesError(t *testing.T) {
	t.Parallel()
	writer := &fakeLegacySchemaWriter{
		fakeLegacySchemaReader: fakeLegacySchemaReader{
			namespaces: []datastore.RevisionedNamespace{
				{Definition: &core.NamespaceDefinition{Name: "old"}, LastWrittenRevision: testRevision},
			},
		},
		deleteNSErr: errors.New("delete ns fail"),
	}

	err := writeSchemaViaLegacy(t.Context(), writer, &writer.fakeLegacySchemaReader, nil)
	require.ErrorContains(t, err, "delete ns fail")
}

func TestWriteSchemaViaLegacy_DeleteCaveatsError(t *testing.T) {
	t.Parallel()
	writer := &fakeLegacySchemaWriter{
		fakeLegacySchemaReader: fakeLegacySchemaReader{
			caveats: []datastore.RevisionedCaveat{
				{Definition: &core.CaveatDefinition{Name: "old"}, LastWrittenRevision: testRevision},
			},
		},
		deleteCavErr: errors.New("delete cav fail"),
	}

	err := writeSchemaViaLegacy(t.Context(), writer, &writer.fakeLegacySchemaReader, nil)
	require.ErrorContains(t, err, "delete cav fail")
}

// --- SchemaReaderFromLegacy interface compliance ---

func TestSchemaReaderFromLegacy_ReturnsSchemaReader(t *testing.T) {
	t.Parallel()
	reader := &fakeLegacySchemaReader{}
	adapter := SchemaReaderFromLegacy(reader)
	require.NotNil(t, adapter)

	// Verify it satisfies the interface.
	_ = adapter
}

// --- newStoredSchemaReaderAdapter cache tests ---

type fakeSchemaCache struct {
	loaded      int
	setCount    int
	cachedValue *datastore.ReadOnlyStoredSchema
}

func (m *fakeSchemaCache) GetOrLoad(ctx context.Context, _ datastore.Revision, _ SchemaHash,
	loader func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error),
) (*datastore.ReadOnlyStoredSchema, error) {
	m.loaded++
	if m.cachedValue != nil {
		return m.cachedValue, nil
	}
	return loader(ctx)
}

func (m *fakeSchemaCache) Set(_ SchemaHash, schema *datastore.ReadOnlyStoredSchema) error {
	m.setCount++
	m.cachedValue = schema
	return nil
}

func TestNewStoredSchemaReaderAdapter_UsesCache(t *testing.T) {
	t.Parallel()
	schema := datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
		Version: 1,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText: "definition user {}",
				NamespaceDefinitions: map[string]*core.NamespaceDefinition{
					"user": {Name: "user"},
				},
			},
		},
	})

	cache := &fakeSchemaCache{cachedValue: schema}
	reader := &fakeStoredSchemaReader{err: errors.New("should not be called")}

	adapter, err := newStoredSchemaReaderAdapter(t.Context(), reader, "hash1", testRevision, cache)
	require.NoError(t, err)
	require.Equal(t, 1, cache.loaded)

	text, err := adapter.SchemaText(t.Context())
	require.NoError(t, err)
	require.Equal(t, "definition user {}", text)
}
