package checkbaseline

import (
	"context"
	"fmt"
	"strings"

	"github.com/authzed/spicedb/internal/datastore/common"
	bm "github.com/authzed/spicedb/pkg/benchmarks"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

type Scale struct {
	Name                               string
	Fanout, Depth, DirectRelationships int
}

func DefaultScales() []Scale {
	return []Scale{{"small", 10, 3, 100}, {"medium", 100, 10, 1000}, {"large", 1000, 30, 10000}, {"boundary99", 99, 3, 99}, {"boundary101", 101, 3, 101}}
}

func check(id, resource, permission, subject string, outcome Outcome) Case {
	p := strings.SplitN(resource, ":", 2)
	return Case{ID: id, Query: bm.CheckQuery{ResourceType: p[0], ResourceID: p[1], Permission: permission, SubjectType: "user", SubjectID: subject, SubjectRelation: tuple.Ellipsis}, Expected: Decision{Outcome: outcome}, ClassicDepth: 200, QPDepth: 50}
}

func GeneratedDatasets(scales []Scale) []Dataset {
	out := make([]Dataset, 0, 8*len(scales))
	for _, scale := range scales {
		for _, family := range []string{"direct", "arrow", "groups", "recursive", "union", "intersection", "exclusion", "all"} {
			effective := scale
			switch family {
			case "direct", "union", "intersection", "exclusion":
				effective.Fanout = 0
				effective.Depth = 0
			case "recursive":
				effective.Fanout = 0
				effective.DirectRelationships = 0
			default:
				effective.Depth = 0
				effective.DirectRelationships = 0
			}
			out = append(out, Dataset{ID: "generated/" + family + "/" + scale.Name, Family: family, Source: "deterministic generator v2", Scale: &effective, Setup: func(ctx context.Context, ds datastore.Datastore) ([]Case, error) {
				schemaText := "definition user {}\ndefinition group { relation member: user | group#member }\ndefinition document { relation viewer: user relation other: user relation banned: user relation parent: document relation group: group permission view = viewer }"
				var rels []tuple.Relationship
				add := func(s string) { rels = append(rels, tuple.MustParse(s)) }
				cases := []Case{check("hit", "document:doc", "view", "target", Allow), check("miss", "document:doc", "view", "absent", Deny)}
				switch family {
				case "direct":
					for i := 0; i < scale.DirectRelationships; i++ {
						add(fmt.Sprintf("document:doc#viewer@user:u%06d", i))
					}
					add("document:doc#viewer@user:target")
				case "arrow", "all", "groups":
					switch family {
					case "groups":
						schemaText = strings.Replace(schemaText, "relation viewer: user", "relation viewer: user | group#member", 1)
					case "all":
						schemaText = strings.Replace(schemaText, "view = viewer", "view = group.all(member)", 1)
					default:
						schemaText = strings.Replace(schemaText, "view = viewer", "view = group->member", 1)
					}
					for i := 0; i < scale.Fanout; i++ {
						if family == "groups" {
							add(fmt.Sprintf("document:doc#viewer@group:g%06d#member", i))
						} else {
							add(fmt.Sprintf("document:doc#group@group:g%06d", i))
						}
						add(fmt.Sprintf("group:g%06d#member@user:all", i))
					}
					add(fmt.Sprintf("group:g%06d#member@user:target", scale.Fanout-1))
					add("group:g000000#member@user:early")
					if family == "all" {
						cases[0] = check("all-hit", "document:doc", "view", "all", Allow)
						cases = append(cases, check("partial-deny", "document:doc", "view", "target", Deny))
					} else {
						cases = append(cases, check("early-hit", "document:doc", "view", "early", Allow))
					}
				case "recursive":
					schemaText = strings.Replace(schemaText, "view = viewer", "view = viewer + parent->view", 1)
					add("document:doc#parent@document:d0")
					for i := 0; i < scale.Depth-1; i++ {
						add(fmt.Sprintf("document:d%d#parent@document:d%d", i, i+1))
					}
					add(fmt.Sprintf("document:d%d#viewer@user:target", scale.Depth-1))
				case "union", "intersection", "exclusion":
					for i := 0; i < scale.DirectRelationships; i++ {
						add(fmt.Sprintf("document:doc#viewer@user:noise%06d", i))
						add(fmt.Sprintf("document:doc#other@user:noise%06d", i))
					}

					op := map[string]string{"union": "+", "intersection": "&", "exclusion": "-"}[family]
					schemaText = strings.Replace(schemaText, "view = viewer", "view = viewer "+op+" other", 1)
					add("document:doc#viewer@user:target")
					if family == "intersection" {
						add("document:doc#other@user:target")
					}
					add("document:doc#other@user:other")
					cases = append(cases, check("other", "document:doc", "view", "other", Deny))
					if family == "union" {
						cases[len(cases)-1].Expected.Outcome = Allow
					}
				}
				schemaText = strings.NewReplacer(" relation ", "\nrelation ", " permission ", "\npermission ", " }", "\n}").Replace(schemaText)
				if _, err := datalayer.WriteStoredSchemaForTest(ctx, ds, schemaText); err != nil {
					return nil, err
				}
				for start := 0; start < len(rels); start += 500 {
					end := min(start+500, len(rels))
					if _, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rels[start:end]...); err != nil {
						return nil, err
					}
				}
				return cases, nil
			}})
		}
	}
	return out
}
