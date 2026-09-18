package architecturetest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	llmImportPath      = modulePath + "/llm"
	providerImportPath = llmImportPath + "/provider"
)

type temporalStreamingImports struct {
	llm         map[string]struct{}
	provider    map[string]struct{}
	dotLLM      bool
	dotProvider bool
}

func TestTemporalOneShotActivityBoundary(t *testing.T) {
	activities := parseGoFile(t, filepath.Join(moduleRoot(t), "activity", "activities.go"))
	if err := validateActivitiesOneShotBoundary(activities); err != nil {
		t.Fatal(err)
	}

	if err := validateNoTemporalStreamingReferences(moduleRoot(t)); err != nil {
		t.Fatal(err)
	}

	runtime := parseGoFile(t, filepath.Join(moduleRoot(t), "internal", "runtime", "runtime.go"))
	if err := validateRuntimeActivityWiring(runtime); err != nil {
		t.Fatal(err)
	}
}

func TestTemporalOneShotActivityBoundaryRejectsStreamingFixtures(t *testing.T) {
	activitiesPath := filepath.Join(moduleRoot(t), "activity", "activities.go")
	activitiesSource := readArchitectureSource(t, activitiesPath)
	runtimePath := filepath.Join(moduleRoot(t), "internal", "runtime", "runtime.go")
	runtimeSource := readArchitectureSource(t, runtimePath)

	for _, test := range []struct {
		name           string
		source         string
		validator      func(*ast.File) error
		wantReferences []string
	}{
		{
			name:           "activity engine narrowed to StreamingEngine",
			source:         replaceArchitectureSource(t, activitiesSource, "Engine                     llm.Engine", "Engine                     llm.StreamingEngine"),
			validator:      validateActivitiesOneShotBoundary,
			wantReferences: []string{"StreamingEngine"},
		},
		{
			name:           "activity dispatches Stream",
			source:         replaceArchitectureSource(t, activitiesSource, "activities.Engine.Generate(generateContext, request)", "activities.Engine.Stream(generateContext, request)"),
			validator:      validateActivitiesOneShotBoundary,
			wantReferences: []string{"Stream"},
		},
		{
			name:           "activity dispatches OpenStream",
			source:         replaceArchitectureSource(t, activitiesSource, "activities.Engine.Generate(generateContext, request)", "activities.Engine.OpenStream(generateContext, request)"),
			validator:      validateActivitiesOneShotBoundary,
			wantReferences: []string{"OpenStream"},
		},
		{
			name: "activity declares Stream API",
			source: activitiesSource + `

func (activities *Activities) Stream(ctx context.Context) {}
`,
			validator:      validateActivitiesOneShotBoundary,
			wantReferences: []string{"Stream"},
		},
		{
			name: "activity declares OpenStream API",
			source: activitiesSource + `

func (activities *Activities) OpenStream(ctx context.Context) {}
`,
			validator:      validateActivitiesOneShotBoundary,
			wantReferences: []string{"OpenStream"},
		},
		{
			name: "activity captures Stream method value",
			source: replaceArchitectureSource(t, activitiesSource,
				"response, err := activities.Engine.Generate(generateContext, request)",
				"stream := activities.Engine.Stream\n\tresponse, err := stream(generateContext, request)"),
			validator:      validateActivitiesOneShotBoundary,
			wantReferences: []string{"Stream"},
		},
		{
			name: "activity dispatches Stream through engine alias",
			source: replaceArchitectureSource(t, activitiesSource,
				"response, err := activities.Engine.Generate(generateContext, request)",
				"engine := activities.Engine\n\tresponse, err := engine.Stream(generateContext, request)"),
			validator:      validateActivitiesOneShotBoundary,
			wantReferences: []string{"Stream"},
		},
		{
			name: "activity dispatches Stream through receiver alias",
			source: replaceArchitectureSource(t, activitiesSource,
				"response, err := activities.Engine.Generate(generateContext, request)",
				"alias := activities\n\tresponse, err := alias.Engine.Stream(generateContext, request)"),
			validator:      validateActivitiesOneShotBoundary,
			wantReferences: []string{"Stream"},
		},
		{
			name:           "activity engine is EventStream",
			source:         replaceArchitectureSource(t, activitiesSource, "Engine                     llm.Engine", "Engine                     llm.EventStream"),
			validator:      validateActivitiesOneShotBoundary,
			wantReferences: []string{"EventStream"},
		},
		{
			name:           "activity engine is StreamingAdapter",
			source:         replaceArchitectureSource(t, activitiesSource, "Engine                     llm.Engine", "Engine                     provider.StreamingAdapter"),
			validator:      validateActivitiesOneShotBoundary,
			wantReferences: []string{"StreamingAdapter"},
		},
		{
			name:      "runtime accepts StreamingEngine",
			source:    replaceArchitectureSource(t, runtimeSource, "dynamic llm.Engine", "dynamic llm.StreamingEngine"),
			validator: validateRuntimeActivityWiring,
		},
		{
			name:      "runtime does not wire dynamic engine",
			source:    replaceArchitectureSource(t, runtimeSource, "Engine:                      dynamic", "Engine:                      replacement"),
			validator: validateRuntimeActivityWiring,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			file := parseArchitectureSource(t, test.name+".go", test.source)
			for _, reference := range test.wantReferences {
				if !containsString(forbiddenTemporalStreamingReferences(file), reference) {
					t.Fatalf("streaming fixture did not report %s", reference)
				}
			}
			if err := test.validator(file); err == nil {
				t.Fatal("streaming fixture was accepted")
			}
		})
	}
}

func TestTemporalOneShotActivityBoundaryRejectsAliasedStreamingTypes(t *testing.T) {
	file := parseArchitectureSource(t, "aliased-types.go", `package activity

import (
	model "github.com/mfow/llm-temporal-worker/golang/llm"
	adapter "github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

type streamingReferences struct {
	engine model.StreamingEngine
	events model.EventStream
	adapter adapter.StreamingAdapter
}
`)
	for _, reference := range []string{"StreamingEngine", "EventStream", "StreamingAdapter"} {
		if !containsString(forbiddenTemporalStreamingReferences(file), reference) {
			t.Fatalf("aliased streaming type did not report %s", reference)
		}
	}
}

func TestTemporalOneShotActivityBoundaryIgnoresCommentsStringsAndHarmlessIdentifiers(t *testing.T) {
	file := parseArchitectureSource(t, "false-positive.go", `package activity

// StreamingEngine EventStream StreamingAdapter OpenStream Stream are library terms.
func example() string {
	StreamingEngine := 1
	EventStream := 2
	StreamingAdapter := 3
	OpenStream := 4
	Stream := 5
	_ = StreamingEngine
	_ = EventStream
	_ = StreamingAdapter
	_ = OpenStream
	_ = Stream
	return "StreamingEngine EventStream StreamingAdapter OpenStream Stream"
}
`)
	if references := forbiddenTemporalStreamingReferences(file); len(references) != 0 {
		t.Fatalf("comments, strings, and harmless identifiers must not count as Temporal streaming references: %v", references)
	}
}

func TestTemporalOneShotActivityBoundaryAllowsUnrelatedStreamSelector(t *testing.T) {
	file := parseArchitectureSource(t, "unrelated-selector.go", `package activity

type Activities struct{}

func (activities *Activities) unrelatedSelector() {
	other := struct{ Stream func() }{}
	_ = other.Stream
}
`)
	if references := forbiddenTemporalStreamingReferences(file); len(references) != 0 {
		t.Fatalf("unrelated Stream selector must not count as a Temporal streaming reference: %v", references)
	}
}

func validateActivitiesOneShotBoundary(file *ast.File) error {
	activities, err := findNamedStruct(file, "Activities")
	if err != nil {
		return err
	}
	if !hasExactlyNamedSelectorField(activities, "Engine", "llm", "Engine") {
		return fmt.Errorf("Activities.Engine must be exactly llm.Engine")
	}

	generate, receiver, err := findMethod(file, "Activities", "Generate")
	if err != nil {
		return err
	}
	if calls := directEngineGenerateCalls(generate, receiver); calls != 1 {
		return fmt.Errorf("Activities.Generate must make exactly one direct %s.Engine.Generate call; found %d", receiver, calls)
	}
	if references := forbiddenTemporalStreamingReferences(file); len(references) != 0 {
		return fmt.Errorf("Activity production code must not reference streaming APIs: %s", strings.Join(references, ", "))
	}
	return nil
}

func validateNoTemporalStreamingReferences(root string) error {
	paths, err := filepath.Glob(filepath.Join(root, "activity", "*.go"))
	if err != nil {
		return err
	}
	for _, filePath := range paths {
		if strings.HasSuffix(filePath, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filePath, nil, 0)
		if err != nil {
			return fmt.Errorf("parse %s: %w", filepath.Base(filePath), err)
		}
		if references := forbiddenTemporalStreamingReferences(file); len(references) != 0 {
			return fmt.Errorf("%s references streaming APIs: %s", filepath.Base(filePath), strings.Join(references, ", "))
		}
	}
	return nil
}

func validateRuntimeActivityWiring(file *ast.File) error {
	function, err := findFunction(file, "newRuntimeActivities")
	if err != nil {
		return err
	}
	if !hasExactlyNamedSelectorParameter(function.Type.Params, "dynamic", "llm", "Engine") {
		return fmt.Errorf("newRuntimeActivities must accept dynamic llm.Engine")
	}

	wired := 0
	ast.Inspect(function.Body, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || !isNamedSelector(literal.Type, "activity", "Activities") {
			return true
		}
		for _, element := range literal.Elts {
			field, ok := element.(*ast.KeyValueExpr)
			if !ok || !isNamedIdentifier(field.Key, "Engine") || !isNamedIdentifier(field.Value, "dynamic") {
				continue
			}
			wired++
		}
		return true
	})
	if wired != 1 {
		return fmt.Errorf("newRuntimeActivities must wire dynamic into activity.Activities.Engine exactly once; found %d", wired)
	}
	return nil
}

func findNamedStruct(file *ast.File, name string) (*ast.StructType, error) {
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.TYPE {
			continue
		}
		for _, specification := range general.Specs {
			typeSpec, ok := specification.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != name {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				return nil, fmt.Errorf("%s must be a struct", name)
			}
			return structType, nil
		}
	}
	return nil, fmt.Errorf("type %s not found", name)
}

func findMethod(file *ast.File, receiverType, name string) (*ast.FuncDecl, string, error) {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != name || function.Recv == nil || len(function.Recv.List) != 1 || len(function.Recv.List[0].Names) != 1 {
			continue
		}
		if namedType(function.Recv.List[0].Type) == receiverType {
			return function, function.Recv.List[0].Names[0].Name, nil
		}
	}
	return nil, "", fmt.Errorf("method %s.%s not found", receiverType, name)
}

func findFunction(file *ast.File, name string) (*ast.FuncDecl, error) {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Recv == nil && function.Name.Name == name {
			return function, nil
		}
	}
	return nil, fmt.Errorf("function %s not found", name)
}

func directEngineGenerateCalls(function *ast.FuncDecl, receiver string) int {
	calls := 0
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !isNamedSelector(call.Fun, "", "Generate") {
			return true
		}
		selector := call.Fun.(*ast.SelectorExpr)
		engine, ok := selector.X.(*ast.SelectorExpr)
		if ok && engine.Sel.Name == "Engine" && isNamedIdentifier(engine.X, receiver) {
			calls++
		}
		return true
	})
	return calls
}

func hasExactlyNamedSelectorField(structType *ast.StructType, name, packageName, typeName string) bool {
	found := 0
	for _, field := range structType.Fields.List {
		if len(field.Names) == 1 && field.Names[0].Name == name && isNamedSelector(field.Type, packageName, typeName) {
			found++
		}
	}
	return found == 1
}

func hasExactlyNamedSelectorParameter(fields *ast.FieldList, name, packageName, typeName string) bool {
	if fields == nil {
		return false
	}
	found := 0
	for _, field := range fields.List {
		if len(field.Names) == 1 && field.Names[0].Name == name && isNamedSelector(field.Type, packageName, typeName) {
			found++
		}
	}
	return found == 1
}

func forbiddenTemporalStreamingReferences(file *ast.File) []string {
	found := make(map[string]struct{})
	imports := streamingImports(file)
	for _, reference := range activitiesStreamingMethodReferences(file) {
		found[reference] = struct{}{}
	}
	for _, reference := range activitiesEngineStreamingReferences(file) {
		found[reference] = struct{}{}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.SelectorExpr:
			if imports.isStreamingType(selectorPackageName(typed), typed.Sel.Name) {
				found[typed.Sel.Name] = struct{}{}
			}
		case *ast.Field:
			imports.addDotImportedStreamingTypes(typed.Type, found)
		case *ast.TypeSpec:
			imports.addDotImportedStreamingTypes(typed.Type, found)
		case *ast.ValueSpec:
			imports.addDotImportedStreamingTypes(typed.Type, found)
		case *ast.TypeAssertExpr:
			imports.addDotImportedStreamingTypes(typed.Type, found)
		}
		return true
	})
	references := make([]string, 0, len(found))
	for reference := range found {
		references = append(references, reference)
	}
	sort.Strings(references)
	return references
}

func activitiesStreamingMethodReferences(file *ast.File) []string {
	found := make(map[string]struct{})
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv == nil || len(function.Recv.List) != 1 || namedType(function.Recv.List[0].Type) != "Activities" {
			continue
		}
		switch function.Name.Name {
		case "Stream", "OpenStream":
			found[function.Name.Name] = struct{}{}
		}
	}
	references := make([]string, 0, len(found))
	for reference := range found {
		references = append(references, reference)
	}
	sort.Strings(references)
	return references
}

func activitiesEngineStreamingReferences(file *ast.File) []string {
	found := make(map[string]struct{})
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv == nil || len(function.Recv.List) != 1 || len(function.Recv.List[0].Names) != 1 {
			continue
		}
		if namedType(function.Recv.List[0].Type) != "Activities" {
			continue
		}
		receiver := function.Recv.List[0].Names[0].Name
		receiverAliases := activityReceiverAliases(function.Body, receiver)
		aliases := activityEngineAliases(function.Body, receiverAliases)
		ast.Inspect(function.Body, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if ok && isActivitiesEngineStreamingSelector(selector, receiverAliases, aliases) {
				found[selector.Sel.Name] = struct{}{}
			}
			return true
		})
	}
	references := make([]string, 0, len(found))
	for reference := range found {
		references = append(references, reference)
	}
	sort.Strings(references)
	return references
}

func activityReceiverAliases(body *ast.BlockStmt, receiver string) map[string]struct{} {
	aliases := map[string]struct{}{receiver: {}}
	for changed := true; changed; {
		changed = false
		ast.Inspect(body, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.AssignStmt:
				for index, value := range typed.Rhs {
					if index >= len(typed.Lhs) || !isActivityReceiverValue(value, aliases) {
						continue
					}
					if identifier, ok := typed.Lhs[index].(*ast.Ident); ok && identifier.Name != "_" {
						if _, exists := aliases[identifier.Name]; !exists {
							aliases[identifier.Name] = struct{}{}
							changed = true
						}
					}
				}
			case *ast.ValueSpec:
				for index, value := range typed.Values {
					if index >= len(typed.Names) || !isActivityReceiverValue(value, aliases) {
						continue
					}
					identifier := typed.Names[index]
					if identifier.Name != "_" {
						if _, exists := aliases[identifier.Name]; !exists {
							aliases[identifier.Name] = struct{}{}
							changed = true
						}
					}
				}
			}
			return true
		})
	}
	return aliases
}

func activityEngineAliases(body *ast.BlockStmt, receiverAliases map[string]struct{}) map[string]struct{} {
	aliases := make(map[string]struct{})
	for changed := true; changed; {
		changed = false
		ast.Inspect(body, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.AssignStmt:
				for index, value := range typed.Rhs {
					if index >= len(typed.Lhs) || !isActivityEngineValue(value, receiverAliases, aliases) {
						continue
					}
					if identifier, ok := typed.Lhs[index].(*ast.Ident); ok && identifier.Name != "_" {
						if _, exists := aliases[identifier.Name]; !exists {
							aliases[identifier.Name] = struct{}{}
							changed = true
						}
					}
				}
			case *ast.ValueSpec:
				for index, value := range typed.Values {
					if index >= len(typed.Names) || !isActivityEngineValue(value, receiverAliases, aliases) {
						continue
					}
					identifier := typed.Names[index]
					if identifier.Name != "_" {
						if _, exists := aliases[identifier.Name]; !exists {
							aliases[identifier.Name] = struct{}{}
							changed = true
						}
					}
				}
			}
			return true
		})
	}
	return aliases
}

func isActivityReceiverValue(expression ast.Expr, aliases map[string]struct{}) bool {
	identifier, ok := expression.(*ast.Ident)
	if !ok {
		return false
	}
	_, alias := aliases[identifier.Name]
	return alias
}

func isActivityEngineValue(expression ast.Expr, receiverAliases, aliases map[string]struct{}) bool {
	if identifier, ok := expression.(*ast.Ident); ok {
		_, alias := aliases[identifier.Name]
		return alias
	}
	selector, ok := expression.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "Engine" && isActivityReceiverValue(selector.X, receiverAliases)
}

func isActivitiesEngineStreamingSelector(selector *ast.SelectorExpr, receiverAliases, aliases map[string]struct{}) bool {
	if selector.Sel.Name != "Stream" && selector.Sel.Name != "OpenStream" {
		return false
	}
	engine, ok := selector.X.(*ast.SelectorExpr)
	if ok {
		return engine.Sel.Name == "Engine" && isActivityReceiverValue(engine.X, receiverAliases)
	}
	identifier, ok := selector.X.(*ast.Ident)
	if !ok {
		return false
	}
	_, alias := aliases[identifier.Name]
	return alias
}

func streamingImports(file *ast.File) temporalStreamingImports {
	imports := temporalStreamingImports{llm: make(map[string]struct{}), provider: make(map[string]struct{})}
	for _, importSpec := range file.Imports {
		importPath, err := strconv.Unquote(importSpec.Path.Value)
		if err != nil {
			continue
		}
		var aliases map[string]struct{}
		var dot *bool
		switch importPath {
		case llmImportPath:
			aliases, dot = imports.llm, &imports.dotLLM
		case providerImportPath:
			aliases, dot = imports.provider, &imports.dotProvider
		default:
			continue
		}
		name := path.Base(importPath)
		if importSpec.Name != nil {
			name = importSpec.Name.Name
		}
		switch name {
		case "_":
		case ".":
			*dot = true
		default:
			aliases[name] = struct{}{}
		}
	}
	return imports
}

func (imports temporalStreamingImports) isStreamingType(packageName, name string) bool {
	if _, imported := imports.llm[packageName]; imported && (name == "StreamingEngine" || name == "EventStream") {
		return true
	}
	_, imported := imports.provider[packageName]
	return imported && name == "StreamingAdapter"
}

func (imports temporalStreamingImports) addDotImportedStreamingTypes(expression ast.Expr, found map[string]struct{}) {
	if expression == nil || (!imports.dotLLM && !imports.dotProvider) {
		return
	}
	ast.Inspect(expression, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		if imports.dotLLM && (identifier.Name == "StreamingEngine" || identifier.Name == "EventStream") {
			found[identifier.Name] = struct{}{}
		}
		if imports.dotProvider && identifier.Name == "StreamingAdapter" {
			found[identifier.Name] = struct{}{}
		}
		return true
	})
}

func selectorPackageName(selector *ast.SelectorExpr) string {
	identifier, _ := selector.X.(*ast.Ident)
	if identifier == nil {
		return ""
	}
	return identifier.Name
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func isNamedSelector(expression ast.Expr, packageName, name string) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	if packageName == "" {
		return true
	}
	return isNamedIdentifier(selector.X, packageName)
}

func isNamedIdentifier(expression ast.Expr, name string) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == name
}

func namedType(expression ast.Expr) string {
	if pointer, ok := expression.(*ast.StarExpr); ok {
		expression = pointer.X
	}
	identifier, _ := expression.(*ast.Ident)
	if identifier == nil {
		return ""
	}
	return identifier.Name
}

func parseGoFile(t *testing.T, path string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return file
}

func parseArchitectureSource(t *testing.T, name, source string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, source, 0)
	if err != nil {
		t.Fatalf("parse synthetic %s: %v", name, err)
	}
	return file
}

func readArchitectureSource(t *testing.T, path string) string {
	t.Helper()
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(source)
}

func replaceArchitectureSource(t *testing.T, source, before, after string) string {
	t.Helper()
	updated := strings.Replace(source, before, after, 1)
	if updated == source {
		t.Fatalf("synthetic fixture did not match %q", before)
	}
	return updated
}
