package schema

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// MaxSchemaApplications bounds how many subschemas the validator can apply to
// any one instance location. allOf, anyOf, oneOf, not, if/then/else and $ref
// all apply at the same location, so their branch counts multiply, and a
// $ref that recurses into a child location repeats that product at every
// level of the instance. A tiny answer could otherwise cost exponential time
// to validate after it was paid for (#1521). Parse rejects a schema whose
// worst case exceeds the bound, at any instance depth up to the depth limit.
const MaxSchemaApplications = 1000

// MaxValidationWork bounds one validation's total work: the number of
// instance values times the schema's worst-case applications per value.
const MaxValidationWork = 2_000_000

// FanoutError reports a schema whose worst-case validation work exceeds
// MaxSchemaApplications, or that references itself in place.
type FanoutError struct {
	Path   string
	Reason string
}

func (err *FanoutError) Error() string {
	return fmt.Sprintf("JSON Schema at %s %s", displayPath(err.Path), err.Reason)
}

type fanoutNode struct {
	// id is the node's index in the graph; bucket signatures use it because
	// paths are caller-controlled text.
	id        int
	path      string
	value     map[string]any
	inPlace   []*fanoutNode
	props     map[string][]*fanoutNode
	wildProps []*fanoutNode
	// prefix holds the prefixItems schemas by array index; items applies to
	// every later index.
	prefix []*fanoutNode
	items  *fanoutNode
	names  []*fanoutNode
	// branches holds then and else, which apply as alternatives and only
	// when the schema has an if.
	branches []*fanoutNode
	// unresolved records a $ref this analysis cannot follow. It matters
	// only if validation can reach the node.
	unresolved bool
}

type fanoutGraph struct {
	root      map[string]any
	nodes     map[string]*fanoutNode
	resources map[string]string            // node path -> enclosing resource root path
	anchors   map[string]map[string]string // resource root -> anchor -> node path
	pending   []*fanoutNode
}

// schemaFanout returns the worst-case number of subschema applications at
// one instance location for an instance at most maxDepth deep.
func schemaFanout(root map[string]any, maxDepth int) (int, error) {
	graph := &fanoutGraph{root: root, nodes: map[string]*fanoutNode{}, resources: map[string]string{}, anchors: map[string]map[string]string{}}
	graph.index(root, "", "")
	rootNode := graph.node("")
	for len(graph.pending) > 0 {
		node := graph.pending[len(graph.pending)-1]
		graph.pending = graph.pending[:len(graph.pending)-1]
		graph.link(node)
	}
	limit := MaxSchemaApplications
	worst := 0
	closures := &inPlaceClosures{memo: map[*fanoutNode]*inPlaceClosure{}, visiting: map[*fanoutNode]bool{}}
	level := []fanoutBucket{{key: "", nodes: map[*fanoutNode]int{rootNode: 1}}}
	for depth := 0; depth <= maxDepth && len(level) > 0; depth++ {
		next := &levelBuilder{buckets: map[string]fanoutBucket{}}
		for _, bucket := range level {
			apps := 0
			reached := map[*fanoutNode]int{}
			for node, count := range bucket.nodes {
				result, err := closures.of(node)
				if err != nil {
					return 0, err
				}
				apps = saturate(apps + saturate(count*result.apps))
				if result.saturated() {
					break
				}
				for target, multiplicity := range result.counts {
					reached[target] = saturate(reached[target] + saturate(count*multiplicity))
				}
			}
			if apps > limit {
				return 0, &FanoutError{Path: bucket.key, Reason: fmt.Sprintf("can apply more than %d subschemas to one instance value", limit)}
			}
			worst = max(worst, apps)
			childBuckets(bucket.key, reached, next)
		}
		level = next.level()
	}
	return worst, nil
}

// maxFanoutBuckets bounds the distinct instance locations tracked per depth.
// Beyond it the locations are merged, which can only overstate the work.
const maxFanoutBuckets = 4096

// levelBuilder collects one depth's child locations, deduplicated by the
// subschemas applied to them. Once it holds maxFanoutBuckets locations it
// folds them, and every later one, into a single merged location, so a
// schema with many distinct combinations costs bounded memory.
type levelBuilder struct {
	buckets map[string]fanoutBucket
	merged  *fanoutBucket
}

// add records one child location; fill reports its subschemas with counts.
func (builder *levelBuilder) add(key string, fill func(func(*fanoutNode, int))) {
	if builder.merged != nil {
		fill(func(node *fanoutNode, count int) {
			builder.merged.nodes[node] = saturate(builder.merged.nodes[node] + count)
		})
		return
	}
	bucket := fanoutBucket{key: key, nodes: map[*fanoutNode]int{}}
	fill(func(node *fanoutNode, count int) { bucket.nodes[node] = saturate(bucket.nodes[node] + count) })
	if len(bucket.nodes) == 0 {
		return
	}
	signature := bucket.signature()
	if existing, ok := builder.buckets[signature]; ok && existing.key <= bucket.key {
		return
	}
	builder.buckets[signature] = bucket
	if len(builder.buckets) > maxFanoutBuckets {
		all := make([]fanoutBucket, 0, len(builder.buckets))
		for _, bucket := range builder.buckets {
			all = append(all, bucket)
		}
		merged := mergeBuckets(all)
		builder.merged, builder.buckets = &merged, nil
	}
}

func (builder *levelBuilder) level() []fanoutBucket {
	if builder.merged != nil {
		return []fanoutBucket{*builder.merged}
	}
	level := make([]fanoutBucket, 0, len(builder.buckets))
	for _, bucket := range builder.buckets {
		level = append(level, bucket)
	}
	return level
}

// fanoutBucket is the multiset of subschemas applied directly (before their
// in-place closure) to one instance location, named by an example key path.
type fanoutBucket struct {
	key   string
	nodes map[*fanoutNode]int
}

// signature identifies the bucket's multiset by node ID and count, so two
// different multisets never share one, whatever text the schema's property
// names hold.
func (bucket fanoutBucket) signature() string {
	entries := make([][2]int, 0, len(bucket.nodes))
	for node, count := range bucket.nodes {
		entries = append(entries, [2]int{node.id, count})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i][0] < entries[j][0] })
	var builder strings.Builder
	for _, entry := range entries {
		builder.WriteString(strconv.Itoa(entry[0]))
		builder.WriteByte(':')
		builder.WriteString(strconv.Itoa(entry[1]))
		builder.WriteByte(',')
	}
	return builder.String()
}

// childBuckets adds to next the subschemas applied to each child location of
// an instance value reached by the given subschemas: each named property, any
// other property, array items, and property names.
func childBuckets(key string, reached map[*fanoutNode]int, next *levelBuilder) {
	names := map[string]bool{}
	for node := range reached {
		for name := range node.props {
			names[name] = true
		}
	}
	add := func(childKey string, pick func(*fanoutNode) []*fanoutNode) {
		next.add(childKey, func(emit func(*fanoutNode, int)) {
			for node, count := range reached {
				for _, child := range pick(node) {
					emit(child, count)
				}
			}
		})
	}
	for name := range names {
		// additionalProperties applies only where its schema lists no such
		// property.
		add(pointerJoin(key, name), func(node *fanoutNode) []*fanoutNode {
			if children, ok := node.props[name]; ok {
				return children
			}
			return node.wildProps
		})
	}
	add(key+"/*", func(node *fanoutNode) []*fanoutNode { return node.wildProps })
	// prefixItems and items apply to disjoint array indices, so each tuple
	// position is its own location and items covers the indices after it.
	positions := 0
	for node := range reached {
		positions = max(positions, len(node.prefix))
	}
	for index := 0; index < positions; index++ {
		add(key+"/"+strconv.Itoa(index), func(node *fanoutNode) []*fanoutNode {
			if index < len(node.prefix) {
				return node.prefix[index : index+1]
			}
			return optionalNode(node.items)
		})
	}
	add(key+"/[]", func(node *fanoutNode) []*fanoutNode { return optionalNode(node.items) })
	add(key+"/<name>", func(node *fanoutNode) []*fanoutNode { return node.names })
}

func optionalNode(node *fanoutNode) []*fanoutNode {
	if node == nil {
		return nil
	}
	return []*fanoutNode{node}
}

func mergeBuckets(buckets []fanoutBucket) fanoutBucket {
	merged := fanoutBucket{key: buckets[0].key, nodes: map[*fanoutNode]int{}}
	for _, bucket := range buckets {
		for node, count := range bucket.nodes {
			merged.nodes[node] = saturate(merged.nodes[node] + count)
		}
	}
	return merged
}

func saturate(value int) int {
	if value < 0 || value > MaxSchemaApplications+1 {
		return MaxSchemaApplications + 1
	}
	return value
}

// maxClosureEntries bounds the memoized in-place closures of one schema, so
// the analysis stays linear in practice on hostile reference graphs.
const maxClosureEntries = 1 << 18

// inPlaceClosure is the multiset of subschemas applied at one node's own
// instance location, the node itself included, and their total. A closure
// above MaxSchemaApplications keeps only its total.
type inPlaceClosure struct {
	counts map[*fanoutNode]int
	apps   int
}

func (closure *inPlaceClosure) saturated() bool { return closure.apps > MaxSchemaApplications }

type inPlaceClosures struct {
	memo     map[*fanoutNode]*inPlaceClosure
	visiting map[*fanoutNode]bool
	entries  int
}

// of returns node's in-place closure. allOf, anyOf, oneOf, not, if and $ref
// all apply, so their closures add; then and else are alternatives, so they
// contribute the element-wise maximum of their closures, which bounds either
// choice. Their subschemas are distinct, so the child locations both reach
// still add: a schema that recurses through both then and else is bounded as
// if both ran, an overstatement that exact per-choice analysis would avoid
// only at exponential cost. An in-place cycle or an unresolvable reachable
// $ref is rejected.
func (closures *inPlaceClosures) of(node *fanoutNode) (*inPlaceClosure, error) {
	if closure, ok := closures.memo[node]; ok {
		return closure, nil
	}
	if closures.visiting[node] {
		return nil, &FanoutError{Path: node.path, Reason: "references itself without consuming instance data"}
	}
	if node.unresolved {
		// The compiler resolved it, so the analysis cannot bound its target.
		return nil, &FanoutError{Path: node.path, Reason: "has a $ref that does not resolve within the schema"}
	}
	closures.visiting[node] = true
	defer delete(closures.visiting, node)
	counts := map[*fanoutNode]int{node: 1}
	saturated := false
	add := func(closure *inPlaceClosure, weight func(*fanoutNode, int) int) {
		if closure.saturated() {
			saturated = true
			return
		}
		for target, count := range closure.counts {
			counts[target] = saturate(weight(target, count))
		}
	}
	for _, child := range node.inPlace {
		closure, err := closures.of(child)
		if err != nil {
			return nil, err
		}
		add(closure, func(target *fanoutNode, count int) int { return counts[target] + count })
	}
	if len(node.branches) > 0 {
		alternatives := map[*fanoutNode]int{}
		for _, branch := range node.branches {
			closure, err := closures.of(branch)
			if err != nil {
				return nil, err
			}
			add(closure, func(target *fanoutNode, count int) int {
				alternatives[target] = max(alternatives[target], count)
				return counts[target]
			})
		}
		for target, count := range alternatives {
			counts[target] = saturate(counts[target] + count)
		}
	}
	apps := 0
	for _, count := range counts {
		apps = saturate(apps + count)
	}
	closure := &inPlaceClosure{counts: counts, apps: apps}
	if saturated || closure.saturated() {
		closure = &inPlaceClosure{apps: MaxSchemaApplications + 1}
	}
	closures.entries += len(closure.counts)
	if closures.entries > maxClosureEntries {
		return nil, &FanoutError{Path: node.path, Reason: "is too complex to bound its validation work"}
	}
	closures.memo[node] = closure
	return closure, nil
}

func (graph *fanoutGraph) node(path string) *fanoutNode {
	return graph.nodes[path]
}

// index records every schema object reachable through schema keywords, the
// resource (nearest $id) each belongs to, and each resource's anchors.
func (graph *fanoutGraph) index(value map[string]any, path, resource string) {
	// An empty $id keeps the enclosing resource, as the compiler does.
	if id, ok := value["$id"].(string); ok && id != "" {
		resource = path
	}
	graph.nodes[path] = &fanoutNode{id: len(graph.nodes), path: path, value: value}
	graph.resources[path] = resource
	graph.pending = append(graph.pending, graph.nodes[path])
	if anchor, ok := value["$anchor"].(string); ok {
		if graph.anchors[resource] == nil {
			graph.anchors[resource] = map[string]string{}
		}
		graph.anchors[resource][anchor] = path
	}
	for key, child := range value {
		childPath := pointerJoin(path, key)
		switch key {
		case "$defs", "properties":
			children, _ := child.(map[string]any)
			for name, nested := range children {
				if object, ok := nested.(map[string]any); ok {
					graph.index(object, pointerJoin(childPath, name), resource)
				}
			}
		case "additionalProperties", "propertyNames", "items", "not", "if", "then", "else":
			if object, ok := child.(map[string]any); ok {
				graph.index(object, childPath, resource)
			} else if _, ok := child.(bool); ok {
				// A boolean schema applies once and has no subschemas, but a
				// $ref may still name it.
				graph.nodes[childPath] = &fanoutNode{id: len(graph.nodes), path: childPath}
				graph.resources[childPath] = resource
			}
		case "prefixItems", "allOf", "anyOf", "oneOf":
			children, _ := child.([]any)
			for index, nested := range children {
				if object, ok := nested.(map[string]any); ok {
					graph.index(object, childPath+"/"+strconv.Itoa(index), resource)
				}
			}
		}
	}
}

// link resolves node's edges. In-place keywords apply at the same instance
// location; the others apply to its properties, items or property names.
func (graph *fanoutGraph) link(node *fanoutNode) {
	child := func(key string) *fanoutNode { return graph.node(pointerJoin(node.path, key)) }
	indexed := func(key string) []*fanoutNode {
		values, _ := node.value[key].([]any)
		nodes := make([]*fanoutNode, 0, len(values))
		for index := range values {
			if target := graph.node(pointerJoin(node.path, key) + "/" + strconv.Itoa(index)); target != nil {
				nodes = append(nodes, target)
			}
		}
		return nodes
	}
	for _, key := range []string{"allOf", "anyOf", "oneOf"} {
		node.inPlace = append(node.inPlace, indexed(key)...)
	}
	for _, key := range []string{"not", "if"} {
		if target := child(key); target != nil {
			node.inPlace = append(node.inPlace, target)
		}
	}
	// The validator ignores then and else without if.
	if _, conditional := node.value["if"]; conditional {
		for _, key := range []string{"then", "else"} {
			if target := child(key); target != nil {
				node.branches = append(node.branches, target)
			}
		}
	}
	if ref, ok := node.value["$ref"].(string); ok {
		if target := graph.resolve(node, ref); target != nil {
			node.inPlace = append(node.inPlace, target)
		} else {
			node.unresolved = true
		}
	}
	if properties, ok := node.value["properties"].(map[string]any); ok {
		node.props = make(map[string][]*fanoutNode, len(properties))
		for name := range properties {
			if target := graph.node(pointerJoin(pointerJoin(node.path, "properties"), name)); target != nil {
				node.props[name] = append(node.props[name], target)
			}
		}
	}
	if target := child("additionalProperties"); target != nil {
		node.wildProps = append(node.wildProps, target)
	}
	node.items = child("items")
	node.prefix = indexed("prefixItems")
	if target := child("propertyNames"); target != nil {
		node.names = append(node.names, target)
	}
}

// resolve follows a same-resource reference: "#", "#/json/pointer" or
// "#anchor", relative to the nearest enclosing $id. It returns nil when the
// target is not a schema this analysis indexed.
func (graph *fanoutGraph) resolve(node *fanoutNode, ref string) *fanoutNode {
	resource := graph.resources[node.path]
	// A fragment is percent-decoded before it is read as a pointer or anchor.
	fragment := unescapeFragment(strings.TrimPrefix(ref, "#"))
	switch {
	case fragment == "":
		return graph.node(resource)
	case strings.HasPrefix(fragment, "/"):
		if target := graph.node(resource + fragment); target != nil {
			return target
		}
		return graph.indexReferenced(resource+fragment, resource)
	default:
		target, ok := graph.anchors[resource][fragment]
		if !ok {
			return nil
		}
		return graph.node(target)
	}
}

// indexReferenced indexes a schema that only a $ref reaches, such as one
// inside a default or examples annotation, which the compiler also accepts.
// It returns nil when the pointer names no schema object or boolean.
func (graph *fanoutGraph) indexReferenced(path, resource string) *fanoutNode {
	var value any = graph.root
	for _, token := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch current := value.(type) {
		case map[string]any:
			value = current[token]
		case []any:
			index, err := strconv.Atoi(token)
			if err != nil || index < 0 || index >= len(current) {
				return nil
			}
			value = current[index]
		default:
			return nil
		}
	}
	switch target := value.(type) {
	case map[string]any:
		// An annotation value escaped the subset check; the analysis counts
		// only subset keywords, so anything else stays unresolved.
		if walkSchema(target, path) != nil {
			return nil
		}
		// Its nodes join the pending list that schemaFanout links.
		graph.index(target, path, resource)
	case bool:
		graph.nodes[path] = &fanoutNode{id: len(graph.nodes), path: path}
		graph.resources[path] = resource
	default:
		return nil
	}
	return graph.node(path)
}

// unescapeFragment percent-decodes a URI fragment JSON pointer. Its ~0 and ~1
// escapes are kept, matching the escaped node paths.
func unescapeFragment(fragment string) string {
	if !strings.Contains(fragment, "%") {
		return fragment
	}
	var builder strings.Builder
	for index := 0; index < len(fragment); index++ {
		if fragment[index] == '%' && index+2 < len(fragment) {
			if value, err := strconv.ParseUint(fragment[index+1:index+3], 16, 8); err == nil {
				builder.WriteByte(byte(value))
				index += 2
				continue
			}
		}
		builder.WriteByte(fragment[index])
	}
	return builder.String()
}
