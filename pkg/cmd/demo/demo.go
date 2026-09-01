package demo

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/cmd/testserver"
)

const (
	DefaultDataSetName = "sample"
	bootstrapFileName  = "demo-bootstrap.yaml"
)

type Config struct {
	TestServerConfig *testserver.Config
	DataSet          string
}

func (c *Config) BootstrapConfig() (map[string][]byte, error) {
	switch c.DataSet {
	case "custom":
		// custom dataset is expected to be provided by the user via --load-configs, so no bootstrap contents are needed
		return nil, nil
	case "", DefaultDataSetName:
		return map[string][]byte{bootstrapFileName: []byte(sampleDatasetYAML())}, nil
	case "basic":
		return map[string][]byte{bootstrapFileName: []byte(basicDatasetYAML())}, nil
	case "docs":
		return map[string][]byte{bootstrapFileName: []byte(docsDatasetYAML())}, nil
	case "entitlements":
		return map[string][]byte{bootstrapFileName: []byte(entitlementsDatasetYAML())}, nil
	case "github":
		return map[string][]byte{bootstrapFileName: []byte(githubDatasetYAML())}, nil
	case "user-defined":
		return map[string][]byte{bootstrapFileName: []byte(userDefinedDatasetYAML())}, nil
	default:
		return nil, fmt.Errorf("unknown demo dataset %q", c.DataSet)
	}
}

func sampleDatasetYAML() string {
	return `---
schema: |
  definition user {}

  definition group {
    relation member: user
  }

  definition document {
    relation reader: user | group#member
    relation writer: user
    permission view = reader + writer
    permission edit = writer
  }

relationships: |-
  group:eng#member@user:alice
  group:eng#member@user:bob
  document:roadmap#reader@group:eng#member
  document:roadmap#writer@user:alice
  document:launch-plan#reader@user:bob
`
}

func basicDatasetYAML() string {
	return `---
schema: |-
  definition user {}
  definition document {
      relation writer: user
      relation reader: user
      permission edit = writer
      permission view = reader + edit
  }

relationships: |-
  document:firstdoc#writer@user:tom
  document:firstdoc#reader@user:fred
  document:seconddoc#reader@user:tom`
}

func docsDatasetYAML() string {
	return `---
schema: |-
  definition user {}

  definition group_with_parent {
     relation parent: group_with_parent
     relation member: user

     permission view = member + parent->member
  }

   definition group_with_child {
    relation child: user | group_with_child#child

    permission view = child
  }

  definition document {
      relation viewer: user | group_with_parent#view | group_with_child#view

      permission view = viewer
  }

relationships: |-
  group_with_parent:engineering#member@user:engineer
  group_with_child:engineering#child@user:engineer
  group_with_parent:analysis#member@user:analyst
  group_with_child:analysis#child@user:analyst

  group_with_parent:engineering#parent@group_with_parent:company
  group_with_child:company#child@group_with_child:engineering#child
  group_with_parent:analysis#parent@group_with_parent:company
  group_with_child:company#child@group_with_child:analysis#child

  document:shared_with_company#viewer@group_with_child:company#view
  document:shared_with_company#viewer@group_with_parent:company#view
  document:shared_with_engineering#viewer@group_with_parent:engineering#view
  document:shared_with_engineering#viewer@group_with_child:engineering#view
  document:shared_with_analysis#viewer@group_with_parent:analysis#view
  document:shared_with_analysis#viewer@group_with_child:analysis#view`
}

func entitlementsDatasetYAML() string {
	return `---
---
schema: |-
  definition user {}

  definition organization {
    relation member: user
  }

  definition entitlement {
    relation org: organization
    permission subscribed_member = org->member
  }

  definition feature {
    relation associated_entitlement: entitlement
    permission access = associated_entitlement->subscribed_member
  }

relationships: |-
  organization:github#member@user:maria
  organization:acme#member@user:frank

  entitlement:pro_plan#org@organization:github
  entitlement:free_plan#org@organization:acme

  feature:download_analytics#associated_entitlement@entitlement:pro_plan
  feature:view_analytics#associated_entitlement@entitlement:free_plan
  feature:view_analytics#associated_entitlement@entitlement:pro_plan`
}

func githubDatasetYAML() string {
	return `---
schema: |-
  definition user {}

  definition team {
      relation parent: organization | team
      relation maintainer: user
      relation direct_member: user

      permission member = maintainer + direct_member
      permission change_team_name = maintainer + parent->change_team_name
  }

  definition organization {
      relation own: user
      relation member: user
      relation billing_manager: user
      relation team_maintainer: user

      permission create_repository = owner + member

      permission manage_billing = owner + billing_manager
      permission user_seat = owner + member + team_maintainer
      permission owner = own

      permission change_team_name = team_maintainer + owner
  }

  definition repository {
      relation organization: organization

      relation reader: user | team#member
      relation triager: user | team#member
      relation writer: user | team#member
      relation maintainer: user | team#member
      relation admin: user | team#member

      permission clone = reader + triager + push
      permission push = writer + maintainer + admin + organization->owner

      permission read = reader + triager + writer + maintainer + admin + organization->owner
      permission delete = admin + organization->owner

      permission create_issue = read
      permission close_issue = triager + writer + maintainer + admin + organization->owner

      permission create_pull_request = read
      permission merge_pull_request = maintainer + organization->owner
      permission close_pull_request = triager + writer + maintainer + admin + organization->owner

      permission manage_setting = maintainer + admin + organization->owner
      permission manage_sensitive_setting = admin + organization->owner
  }
relationships: |-
  repository:authzed_go#organization@organization:authzed#...

  repository:authzed_go#reader@user:jake#...
  repository:authzed_go#admin@user:jimmy#...
  repository:authzed_go#triager@user:jessica#...

  repository:authzed_go#maintainer@team:support_engineers#member

  organization:authzed#own@user:jake#...
  organization:authzed#own@user:jimmy#...

  team:support_engineers#maintainer@user:ivan#...
  team:support_engineers#direct_member@user:ian#...
  team:support_engineers#parent@organization:authzed#...

  team:emea_support_engineers#direct_member@user:iona#...
  team:emea_support_engineers#parent@team:support_engineers#...`
}

func userDefinedDatasetYAML() string {
	return `---
schema: |
  definition user {}

  definition project {
  	relation issue_creator: role#member
  	relation issue_assigner: role#member
  	relation any_issue_resolver: role#member
  	relation assigned_issue_resolver: role#member
  	relation comment_creator: role#member
  	relation comment_deleter: role#member
  	relation role_manager: role#member

  	permission create_issue = issue_creator
  	permission create_role = role_manager
  }

  definition role {
  	relation project: project
  	relation member: user
  	relation built_in_role: project

  	permission delete = project->role_manager - built_in_role->role_manager
  	permission add_user = project->role_manager
  	permission add_permission = project->role_manager - built_in_role->role_manager
  	permission remove_permission = project->role_manager - built_in_role->role_manager
  }

  definition issue {
  	relation project: project
  	relation assigned: user

  	permission assign = project->issue_assigner
  	permission resolve = (project->assigned_issue_resolver & assigned) + project->any_issue_resolver
  	permission create_comment = project->comment_creator

  	permission project_comment_deleter = project->comment_deleter
  }

  definition comment {
  	relation issue: issue
  	permission delete = issue->project_comment_deleter
  }
relationships: |
  issue:move_the_servers#project@project:pied_piper
  issue:move_the_servers#assigned@user:gilfoyle

  issue:too_slow#project@project:pied_piper
  comment:try_middle_out#issue@issue:too_slow

  role:admin#project@project:pied_piper
  role:admin#built_in_role@project:pied_piper
  role:developer#project@project:pied_piper
  role:developer#built_in_role@project:pied_piper
  role:user#project@project:pied_piper
  role:user#built_in_role@project:pied_piper
  role:project_manager#project@project:pied_piper
  role:legal#project@project:pied_piper

  project:pied_piper#issue_creator@role:admin#member
  project:pied_piper#issue_creator@role:developer#member
  project:pied_piper#issue_creator@role:user#member

  project:pied_piper#issue_assigner@role:admin#member
  project:pied_piper#issue_assigner@role:project_manager#member

  project:pied_piper#any_issue_resolver@role:admin#member
  project:pied_piper#any_issue_resolver@role:project_manager#member

  project:pied_piper#assigned_issue_resolver@role:admin#member
  project:pied_piper#assigned_issue_resolver@role:developer#member

  project:pied_piper#comment_creator@role:admin#member
  project:pied_piper#comment_creator@role:developer#member
  project:pied_piper#comment_creator@role:user#member

  project:pied_piper#comment_deleter@role:admin#member
  project:pied_piper#comment_deleter@role:legal#member

  project:pied_piper#role_manager@role:admin#member

  role:admin#member@user:richard
  role:developer#member@user:gilfoyle
  role:user#member@user:monica
  role:project_manager#member@user:jared
  role:legal#member@user:ron`
}
