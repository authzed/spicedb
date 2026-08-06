package decorators

// TestFlag is the `use` feature flag that enables the decorators in TestRegistry.
// It is registered as a valid flag only in test binaries; see pkg/schemadsl/lexer/flags.go,
// which imports this constant so the name is declared exactly once.
const TestFlag = "testdecorators"

// TestRegistry is a fixture registry used to exercise the decorator machinery. It
// deliberately covers combinations no real decorator is expected to have.
//
// It is never used in production: the compiler defaults to DefaultRegistry, and tests
// opt in via compiler.WithDecoratorRegistry.
var TestRegistry = Registry{
	"testdef": {
		Name:         "testdef",
		RequiredFlag: TestFlag,
		Sites:        []Site{SiteDefinition},
	},
	"testrel": {
		Name:         "testrel",
		RequiredFlag: TestFlag,
		Sites:        []Site{SiteRelation, SitePermission},
	},
	"testsub": {
		Name:         "testsub",
		RequiredFlag: TestFlag,
		Sites:        []Site{SiteSubjectType},
	},
	"testcaveat": {
		Name:         "testcaveat",
		RequiredFlag: TestFlag,
		Sites:        []Site{SiteCaveat},
	},
	"testall": {
		Name:         "testall",
		RequiredFlag: TestFlag,
		Sites:        []Site{SiteDefinition, SiteRelation, SitePermission, SiteSubjectType, SiteCaveat},
		Parameters: []Parameter{
			{Name: "needed", Type: ParamTypeInt, Required: true},
			{Name: "count", Type: ParamTypeInt},
			{Name: "label", Type: ParamTypeString},
			{Name: "on", Type: ParamTypeBool},
			{Name: "mode", Type: ParamTypeEnum, EnumValues: []string{"hash", "range"}},
		},
	},
}
