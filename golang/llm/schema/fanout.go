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
	// apps counts the in-place paths from this node, itself included,
	// saturating above MaxSchemaApplications.
	state uint8
	apps  int
	// unresolved records a $ref this analysis cannot follow. It matters
	// only if validation can reach the node.
	unresolved bool
}

type fanoutGraph struct {
	nodes     map[string]*fanoutNode
	resources map[string]string            // node path -> enclosing resource root path
	anchors   map[string]map[string]string // resource root -> anchor -> node path
	pending   []*fanoutNode
}

// schemaFanout returns the worst-case number of subschema applications at
// one instance location for an instance at most maxDepth deep.
func schemaFanout(root map[string]any, maxDepth int) (int, error) {
	graph := &fanoutGraph{nodes: map[string]*fanoutNode{}, resources: map[string]string{}, anchors: map[string]map[string]string{}}
	graph.index(root, "", "")
	rootNode := graph.node("")
	for len(graph.pending) > 0 {
		node := graph.pending[len(graph.pending)-1]
		graph.pending = graph.pending[:len(graph.pending)-1]
		graph.link(node)
	}
	limit := MaxSchemaApplications
	worst := 0
	level := []fanoutBucket{{key: "", nodes: map[*fanoutNode]int{rootNode: 1}}}
	for depth := 0; depth <= maxDepth && len(level) > 0; depth++ {
		next := map[string]fanoutBucket{}
		for _, bucket := range level {
			apps := 0
			for node, count := range bucket.nodes {
				if err := closeInPlace(node); err != nil {
					return 0, err
				}
				apps = saturate(apps + saturate(count*node.apps))
			}
			if apps > limit {
				return 0, &FanoutError{Path: bucket.key, Reason: fmt.Sprintf("can apply more than %d subschemas to one instance value", limit)}
			}
			worst = max(worst, apps)
			// Within the bound, the in-place paths are few: walk them rather
			// than keep every node's closure, which a long $ref chain would
			// make quadratic.
			reached := map[*fanoutNode]int{}
			for node, count := range bucket.nodes {
				walkInPlace(node, count, reached)
			}
			for _, child := range childBuckets(bucket.key, reached) {
				signature := child.signature()
				if existing, ok := next[signature]; ok && existing.key <= child.key {
					continue
				}
				next[signature] = child
			}
		}
		level = level[:0]
		for _, bucket := range next {
			level = append(level, bucket)
		}
		if len(level) > maxFanoutBuckets {
			level = []fanoutBucket{mergeBuckets(level)}
		}
	}
	return worst, nil
}

// maxFanoutBuckets bounds the distinct instance locations tracked per depth.
// Beyond it the locations are merged, which can only overstate the work.
const maxFanoutBuckets = 4096

// fanoutBucket is the multiset of subschemas applied directly (before their
// in-place closure) to one instance location, named by an example key path.
type fanoutBucket struct {
	key   string
	nodes map[*fanoutNode]int
}

func (bucket fanoutBucket) signature() string {
	parts := make([]string, 0, len(bucket.nodes))
	for node, count := range bucket.nodes {
		parts = append(parts, node.path+"\x00"+strconv.Itoa(count))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x01")
}

// childBuckets returns the subschemas applied to each child location of an
// instance value reached by the given subschemas: each named property, any
// other property, array items, and property names.
func childBuckets(key string, reached map[*fanoutNode]int) []fanoutBucket {
	names := map[string]bool{}
	for node := range reached {
		for name := range node.props {
			names[name] = true
		}
	}
	var buckets []fanoutBucket
	add := func(childKey string, pick func(*fanoutNode) []*fanoutNode) {
		nodes := map[*fanoutNode]int{}
		for node, count := range reached {
			for _, child := range pick(node) {
				nodes[child] = saturate(nodes[child] + count)
			}
		}
		if len(nodes) > 0 {
			buckets = append(buckets, fanoutBucket{key: childKey, nodes: nodes})
		}
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
	return buckets
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

// closeInPlace counts the in-place paths from node: the subschemas applied at
// its own instance location, with repeats. It rejects an in-place cycle.
func closeInPlace(node *fanoutNode) error {
	switch node.state {
	case 2:
		return nil
	case 1:
		return &FanoutError{Path: node.path, Reason: "references itself without consuming instance data"}
	}
	if node.unresolved {
		// The compiler resolved it, so the analysis cannot bound its target.
		return &FanoutError{Path: node.path, Reason: "has a $ref that does not resolve within the schema"}
	}
	node.state = 1
	node.apps = 1
	for _, child := range node.inPlace {
		if err := closeInPlace(child); err != nil {
			return err
		}
		node.apps = saturate(node.apps + child.apps)
	}
	node.state = 2
	return nil
}

// walkInPlace adds count to every node on an in-place path from node. The
// caller has checked that those paths number at most MaxSchemaApplications.
func walkInPlace(node *fanoutNode, count int, reached map[*fanoutNode]int) {
	reached[node] = saturate(reached[node] + count)
	for _, child := range node.inPlace {
		walkInPlace(child, count, reached)
	}
}

func (graph *fanoutGraph) node(path string) *fanoutNode {
	return graph.nodes[path]
}

// index records every schema object reachable through schema keywords, the
// resource (nearest $id) each belongs to, and each resource's anchors.
func (graph *fanoutGraph) index(value map[string]any, path, resource string) {
	if _, ok := value["$id"].(string); ok {
		resource = path
	}
	graph.nodes[path] = &fanoutNode{path: path, value: value}
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
				graph.nodes[childPath] = &fanoutNode{path: childPath}
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
	for _, key := range []string{"not", "if", "then", "else"} {
		if target := child(key); target != nil {
			node.inPlace = append(node.inPlace, target)
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
		return graph.node(resource + fragment)
	default:
		target, ok := graph.anchors[resource][fragment]
		if !ok {
			return nil
		}
		return graph.node(target)
	}
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
