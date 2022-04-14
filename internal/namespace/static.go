package namespace

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func NewStaticManager(schema string) (Manager, error) {
	inputSchema := compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schema,
	}

	emptyDefaultPrefix := ""

	nsdefs, err := compiler.Compile([]compiler.InputSchema{inputSchema}, &emptyDefaultPrefix)
	if err != nil {
		return staticNSM{}, fmt.Errorf("unable to compile schema: %w", err)
	}

	nsMap := make(map[string]*core.NamespaceDefinition, len(nsdefs))
	tsMap := make(map[string]*NamespaceTypeSystem, len(nsdefs))
	for _, ns := range nsdefs {
		nsTS, err := BuildNamespaceTypeSystemForDefs(ns, nsdefs)
		if err != nil {
			return staticNSM{}, fmt.Errorf("unable to build type system for namespace `%s`: %w", ns.Name, err)
		}

		tsMap[ns.Name] = nsTS

		strippedNS := proto.Clone(ns).(*core.NamespaceDefinition)
		namespace.FilterUserDefinedMetadataInPlace(strippedNS)

		nsMap[ns.Name] = strippedNS
	}

	log.Info().Int("namespaces", len(nsMap)).Msg("loaded static namespace manager")

	return staticNSM{nsMap, tsMap}, nil
}

type staticNSM struct {
	nsMap map[string]*core.NamespaceDefinition
	tsMap map[string]*NamespaceTypeSystem
}

func (s staticNSM) ReadNamespace(ctx context.Context, nsName string, revision decimal.Decimal) (*core.NamespaceDefinition, error) {
	found, ok := s.nsMap[nsName]
	if !ok {
		return nil, NewNamespaceNotFoundErr(nsName)
	}

	return found, nil
}

func (s staticNSM) ReadNamespaceAndRelation(ctx context.Context, namespace, relation string, revision decimal.Decimal) (*core.NamespaceDefinition, *core.Relation, error) {
	// TODO share impl with caching, or take out of namespace manager
	config, err := s.ReadNamespace(ctx, namespace, revision)
	if err != nil {
		return nil, nil, err
	}

	for _, rel := range config.Relation {
		if rel.Name == relation {
			return config, rel, nil
		}
	}

	return nil, nil, NewRelationNotFoundErr(namespace, relation)
}

func (s staticNSM) CheckNamespaceAndRelation(ctx context.Context, namespace, relation string, allowEllipsis bool, revision decimal.Decimal) error {
	// TODO share impl with caching, or take out of namespace manager
	config, err := s.ReadNamespace(ctx, namespace, revision)
	if err != nil {
		return err
	}

	if allowEllipsis && relation == datastore.Ellipsis {
		return nil
	}

	for _, rel := range config.Relation {
		if rel.Name == relation {
			return nil
		}
	}

	return NewRelationNotFoundErr(namespace, relation)
}

func (s staticNSM) ReadNamespaceAndTypes(ctx context.Context, nsName string, revision decimal.Decimal) (*core.NamespaceDefinition, *NamespaceTypeSystem, error) {
	config, err := s.ReadNamespace(ctx, nsName, revision)
	if err != nil {
		return nil, nil, err
	}

	loadedTS, ok := s.tsMap[nsName]
	if !ok {
		panic(fmt.Sprintf("type system not found for namespace `%s`", nsName))
	}

	return config, loadedTS, nil
}

func (s staticNSM) Close() error {
	// Intentionally blank
	return nil
}

var _ Manager = staticNSM{}
