package parser

import (
	"container/list"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

type testNode struct {
	nodeType   dslshape.NodeType
	properties map[string]interface{}
	children   map[string]*list.List
}

type parserTest struct {
	name     string
	filename string
}

func (pt *parserTest) input() string {
	b, err := os.ReadFile(fmt.Sprintf("tests/%s.zed", pt.filename))
	if err != nil {
		panic(err)
	}

	return string(b)
}

func (pt *parserTest) tree() string {
	b, err := os.ReadFile(fmt.Sprintf("tests/%s.zed.expected", pt.filename))
	if err != nil {
		panic(err)
	}

	return string(b)
}

func (pt *parserTest) writeTree(value string) {
	err := os.WriteFile(fmt.Sprintf("tests/%s.zed.expected", pt.filename), []byte(value), 0o600)
	if err != nil {
		panic(err)
	}
}

func createAstNode(_ input.Source, kind dslshape.NodeType) AstNode {
	return &testNode{
		nodeType:   kind,
		properties: make(map[string]interface{}),
		children:   make(map[string]*list.List),
	}
}

func (tn *testNode) GetType() dslshape.NodeType {
	return tn.nodeType
}

func (tn *testNode) Connect(predicate string, other AstNode) {
	if tn.children[predicate] == nil {
		tn.children[predicate] = list.New()
	}

	tn.children[predicate].PushBack(other)
}

func (tn *testNode) MustDecorate(property string, value string) AstNode {
	if _, ok := tn.properties[property]; ok {
		panic(fmt.Sprintf("Existing key for property %s\n\tNode: %v", property, tn.properties))
	}

	tn.properties[property] = value
	return tn
}

func (tn *testNode) MustDecorateWithInt(property string, value int) AstNode {
	if _, ok := tn.properties[property]; ok {
		panic(fmt.Sprintf("Existing key for property %s\n\tNode: %v", property, tn.properties))
	}

	tn.properties[property] = value
	return tn
}

func TestParser(t *testing.T) {
	parserTests := []parserTest{
		{"empty file test", "empty"},
		{"basic definition test", "basic"},
		{"doc comments test", "doccomments"},
		{"arrow test", "arrow"},
		{"multiple definition test", "multidef"},
		{"broken test", "broken"},
		{"relation missing type test", "relation_missing_type"},
		{"permission missing expression test", "permission_missing_expression"},
		{"relation invalid type test", "relation_invalid_type"},
		{"permission invalid expression test", "permission_invalid_expression"},
		{"cross tenant test", "crosstenant"},
		{"indented comments test", "indentedcomments"},
		{"parens test", "parens"},
		{"multiple parens test", "multiparen"},
		{"multiple slashes in object type", "multipleslashes"},
		{"wildcard test", "wildcard"},
		{"broken wildcard test", "brokenwildcard"},
		{"nil test", "nil"},
		{"caveats type test", "caveatstype"},
		{"basic caveat test", "basiccaveat"},
		{"complex caveat test", "complexcaveat"},
		{"empty caveat test", "emptycaveat"},
		{"unclosed caveat test", "unclosedcaveat"},
		{"invalid caveat expr test", "invalidcaveatexpr"},
		{"associativity test", "associativity"},
		{"super large test", "superlarge"},
		{"invalid permission name test", "invalid_perm_name"},
		{"union positions test", "unionpos"},
		{"arrow operations test", "arrowops"},
		{"arrow illegal operations test", "arrowillegalops"},
		{"arrow illegal function test", "arrowillegalfunc"},
		{"caveat with keyword parameter test", "caveatwithkeywordparam"},
		{"use expiration test", "useexpiration"},
		{"use expiration keyword test", "useexpirationkeyword"},
		{"expiration non-keyword test", "expirationnonkeyword"},
		{"invalid use", "invaliduse"},
		{"use after definition", "useafterdef"},
		{"invalid use expiration test", "invaliduseexpiration"},
	}

	for _, test := range parserTests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := Parse(createAstNode, input.Source(test.name), test.input())
			parseTree := getParseTree((root).(*testNode), 0)
			assert := assert.New(t)

			found := strings.TrimSpace(parseTree)

			if os.Getenv("REGEN") == "true" {
				test.writeTree(found)
			} else {
				expected := strings.TrimSpace(test.tree())
				if !assert.Equal(expected, found, test.name) {
					t.Log(parseTree)
				}
			}
		})
	}
}

func getParseTree(currentNode *testNode, indentation int) string {
	parseTree := ""
	parseTree = parseTree + strings.Repeat(" ", indentation)
	parseTree = parseTree + fmt.Sprintf("%v", currentNode.nodeType)
	parseTree = parseTree + "\n"

	keys := make([]string, 0)

	for key := range currentNode.properties {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		parseTree = parseTree + strings.Repeat(" ", indentation+2)
		parseTree = parseTree + fmt.Sprintf("%s = %v", key, currentNode.properties[key])
		parseTree = parseTree + "\n"
	}

	keys = make([]string, 0)

	for key := range currentNode.children {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		value := currentNode.children[key]
		parseTree = parseTree + fmt.Sprintf("%s%v =>", strings.Repeat(" ", indentation+2), key)
		parseTree = parseTree + "\n"

		for e := value.Front(); e != nil; e = e.Next() {
			parseTree = parseTree + getParseTree(e.Value.(*testNode), indentation+4)
		}
	}

	return parseTree
}
