package query

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/pkg/query/signals/signal"
	schema "github.com/authzed/spicedb/pkg/schema/v2"
)

// StatisticalFacts holds a subtree's structural identity and schema dependencies.
type StatisticalFacts struct {
	// Identity is the full structural identity of the observed subtree.
	Identity string

	// Footprint lists the subtree's transitive schema dependencies.
	Footprint signal.Footprint
}

// DescribeOutline computes full structural identities in one bottom-up traversal.
// Existing canonical keys deliberately omit some children and stay unchanged.
func DescribeOutline(root Outline) map[OutlineNodeID]StatisticalFacts {
	facts := map[OutlineNodeID]StatisticalFacts{}
	definitions := map[signal.Element][]signal.Footprint{}
	var visit func(Outline) StatisticalFacts
	visit = func(node Outline) StatisticalFacts {
		hash := sha256.New()
		hash.Write([]byte{byte(node.Type)})
		elements := map[signal.Element]bool{}
		caveats := map[string]bool{}

		if node.Args != nil {
			hash.Write([]byte(serializeArgs(node.Args)))
			hash.Write([]byte(structuralProto(node.Args.Caveat)))

			for _, path := range node.Args.FixedPaths {
				data, _ := json.Marshal(describeFixedPath(path))
				hash.Write(data)
			}

			a := node.Args
			if a.DefinitionName != "" && a.RelationName != "" {
				elements[signal.Element{Namespace: a.DefinitionName, Relation: a.RelationName}] = true
			}

			for _, name := range a.AliasedAs {
				elements[signal.Element{Namespace: a.DefinitionName, Relation: name}] = true
			}

			if a.Caveat != nil {
				caveats[a.Caveat.CaveatName] = true
			}

			if a.Relation != nil {
				if r, ok := a.Relation.Parent().(*schema.Relation); ok {
					if d, ok := r.Parent().(*schema.Definition); ok {
						elements[signal.Element{Namespace: d.Name(), Relation: r.Name()}] = true
					}
				}

				if c := a.Relation.Caveat(); c != "" {
					caveats[c] = true
				}
			}
		}

		for _, child := range node.SubOutlines {
			f := visit(child)
			hash.Write([]byte(f.Identity))

			for _, e := range f.Footprint.Elements {
				elements[e] = true
			}

			for _, c := range f.Footprint.Caveats {
				caveats[c] = true
			}
		}

		f := StatisticalFacts{Identity: hex.EncodeToString(hash.Sum(nil))}

		for e := range elements {
			f.Footprint.Elements = append(f.Footprint.Elements, e)
		}

		for c := range caveats {
			f.Footprint.Caveats = append(f.Footprint.Caveats, c)
		}

		sort.Slice(f.Footprint.Elements, func(i, j int) bool {
			a, b := f.Footprint.Elements[i], f.Footprint.Elements[j]
			if a.Namespace == b.Namespace {
				return a.Relation < b.Relation
			}

			return a.Namespace < b.Namespace
		})
		sort.Strings(f.Footprint.Caveats)

		if node.Args != nil && (node.Type == AliasIteratorType || node.Type == RecursiveIteratorType) {
			key := signal.Element{Namespace: node.Args.DefinitionName, Relation: node.Args.RelationName}
			definitions[key] = append(definitions[key], f.Footprint)
		}

		facts[node.ID] = f
		return f
	}
	visit(root)

	// Close finite recursive references with cycle detection; collapsed aliases remain dependencies.
	for id, f := range facts {
		elements := map[signal.Element]bool{}
		caveats := map[string]bool{}
		var expand func(signal.Footprint)
		expand = func(p signal.Footprint) {
			for _, name := range p.Caveats {
				caveats[name] = true
			}

			for _, e := range p.Elements {
				if elements[e] {
					continue
				}

				elements[e] = true

				for _, dependency := range definitions[e] {
					expand(dependency)
				}
			}
		}
		expand(f.Footprint)
		f.Footprint = signal.Footprint{}

		for e := range elements {
			f.Footprint.Elements = append(f.Footprint.Elements, e)
		}

		for c := range caveats {
			f.Footprint.Caveats = append(f.Footprint.Caveats, c)
		}

		sort.Slice(f.Footprint.Elements, func(i, j int) bool {
			a, b := f.Footprint.Elements[i], f.Footprint.Elements[j]
			if a.Namespace == b.Namespace {
				return a.Relation < b.Relation
			}

			return a.Namespace < b.Namespace
		})
		sort.Strings(f.Footprint.Caveats)
		facts[id] = f
	}

	return facts
}

// UniqueOutlineOccurrences gives every occurrence a distinct planning identity.
func UniqueOutlineOccurrences(co CanonicalOutline) CanonicalOutline {
	keys := make(map[OutlineNodeID]CanonicalKey, len(co.CanonicalKeys))
	next := OutlineNodeID(1)

	for id, k := range co.CanonicalKeys {
		keys[id] = k
		next = max(next, id+1)
	}

	seen := map[OutlineNodeID]bool{}
	var visit func(Outline) Outline
	visit = func(n Outline) Outline {
		if n.ID == 0 || seen[n.ID] {
			n.ID = next
			next++
			keys[n.ID] = n.Serialize()
		}

		seen[n.ID] = true

		if len(n.SubOutlines) > 0 {
			subs := make([]Outline, len(n.SubOutlines))

			for i, s := range n.SubOutlines {
				subs[i] = visit(s)
			}

			n.SubOutlines = subs
		}

		return n
	}
	co.Root = visit(co.Root)
	co.CanonicalKeys = keys
	return co
}

// ExecutionDescriptor binds compiled occurrence metadata to a concrete iterator.
type ExecutionDescriptor struct {
	StatisticalFacts

	// Strategy identifies the execution strategy used by this population.
	Strategy string

	// Role distinguishes sender wrappers from receiver bodies.
	Role string
}

// ExecutionDescriptors maps iterator occurrences to structural identities and execution strategies.
func ExecutionDescriptors(root Iterator) (map[Iterator]ExecutionDescriptor, error) {
	outline, err := Decompile(root)
	if err != nil {
		return nil, err
	}

	co := UniqueOutlineOccurrences(CanonicalOutline{Root: outline})
	facts := DescribeOutline(co.Root)
	result := map[Iterator]ExecutionDescriptor{}
	var pair func(Iterator, Outline) string
	pair = func(it Iterator, n Outline) string {
		strategies := []string{"default"}
		subs := it.Subiterators()

		for i, sub := range subs {
			child := pair(sub, n.SubOutlines[i])
			if child != "default" {
				strategies = append(strategies, child)
			}
		}

		if a, ok := it.(*ArrowIterator); ok && a.direction == rightToLeft {
			strategies = append(strategies, "right-to-left")
		}

		strategy := strings.Join(strategies, "/")
		role := "body"

		if r, ok := it.(interface{ ObservationRole() string }); ok {
			role = r.ObservationRole()
		}

		result[it] = ExecutionDescriptor{StatisticalFacts: facts[n.ID], Strategy: strategy, Role: role}
		return strategy
	}
	pair(root, co.Root)
	return result, nil
}

func structuralProto(m interface{ CloneMessageVT() proto.Message }) string {
	if m == nil {
		return ""
	}

	data, _ := (proto.MarshalOptions{Deterministic: true}).Marshal(m.CloneMessageVT())
	return string(data)
}

type fixedPathFacts struct {
	// Endpoint identifies the fixed path's resource and subject endpoints.
	Endpoint string

	// Caveat encodes the fixed path's full caveat expression and context.
	Caveat string

	// Expiration encodes the fixed path's expiration in UTC.
	Expiration string

	// Integrity encodes integrity metadata associated with the fixed path.
	Integrity []string

	// Excluded contains the structural facts of excluded subject paths.
	Excluded []fixedPathFacts
}

func describeFixedPath(p Path) fixedPathFacts {
	f := fixedPathFacts{Endpoint: p.Key(), Caveat: structuralProto(p.Caveat)}

	if p.Expiration != nil {
		f.Expiration = p.Expiration.UTC().Format(time.RFC3339Nano)
	}

	for _, item := range p.Integrity {
		f.Integrity = append(f.Integrity, structuralProto(item))
	}

	for _, item := range p.ExcludedSubjects {
		if item != nil {
			f.Excluded = append(f.Excluded, describeFixedPath(*item))
		}
	}

	return f
}
