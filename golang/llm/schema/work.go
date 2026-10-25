package schema

import (
	"encoding/json"
	"regexp/syntax"
)

// Validation work is metered in sixteenths of one subschema application,
// which costs the validator about half a microsecond, so MaxValidationWork
// units take about a second (#1660).
const workScale = 16

// keywordCost weights the keywords whose cost grows with their own size, in
// sixteenths of an application. enum and const compare the instance value
// with each member of its JSON kind: a number comparison parses both sides
// through big.Rat and costs about one application, another scalar far less,
// and an array or object member up to its value count in comparisons.
// required probes each listed name. pattern runs a regular expression whose
// cost grows with its program size times the string's length.
type keywordCost struct {
	members  [jsonKinds]int
	required int
	// pattern is the compiled program's instruction count, or zero.
	pattern int
}

const (
	kindNull = iota
	kindBool
	kindNumber
	kindString
	kindArray
	kindObject
	jsonKinds
)

func jsonKind(value any) int {
	switch value.(type) {
	case nil:
		return kindNull
	case bool:
		return kindBool
	case json.Number, float64:
		return kindNumber
	case string:
		return kindString
	case []any:
		return kindArray
	default:
		return kindObject
	}
}

// memberCost is the cost of comparing an instance value with one enum or
// const member.
func memberCost(member any) int {
	switch jsonKind(member) {
	case kindNumber:
		return workScale
	case kindArray, kindObject:
		return workScale * countValues(member)
	default:
		return 1
	}
}

// keywordCosts returns value's keyword weights, or nil when none applies.
func keywordCosts(value map[string]any) *keywordCost {
	cost := &keywordCost{}
	weighted := false
	if members, ok := value["enum"].([]any); ok {
		for _, member := range members {
			cost.members[jsonKind(member)] += memberCost(member)
		}
		weighted = true
	}
	if member, ok := value["const"]; ok {
		cost.members[jsonKind(member)] += memberCost(member)
		weighted = true
	}
	if names, ok := value["required"].([]any); ok && len(names) > 0 {
		cost.required = len(names)
		weighted = true
	}
	if pattern, ok := value["pattern"].(string); ok {
		cost.pattern = patternInstructions(pattern)
		weighted = true
	}
	if !weighted {
		return nil
	}
	return cost
}

// patternInstructions returns the instruction count of pattern compiled as
// the validator's Go regexp engine compiles it.
func patternInstructions(pattern string) int {
	parsed, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return len(pattern) + 1
	}
	program, err := syntax.Compile(parsed.Simplify())
	if err != nil {
		return len(pattern) + 1
	}
	return len(program.Inst)
}

// cost returns the cost of applying node once to value, the application
// itself included.
func (node *fanoutNode) cost(value any) int {
	cost := workScale
	keywords := node.keywords
	if keywords == nil {
		return cost
	}
	cost += keywords.members[jsonKind(value)]
	switch value := value.(type) {
	case map[string]any:
		cost += keywords.required
	case string:
		if keywords.pattern > 0 {
			// The engine steps through every instruction for each byte;
			// a step costs about a sixty-fourth of an application.
			cost += keywords.pattern * (len(value) + 1) * workScale / 64
		}
	}
	return cost
}

// workPlan bounds one validation's work. It is read-only after Parse, so
// concurrent validations can share it.
type workPlan struct {
	// applications is the worst-case number of subschemas applied to one
	// instance value.
	applications int
	root         *fanoutNode
	// closures holds the in-place closure of every node an instance within
	// the depth limit can reach.
	closures map[*fanoutNode]*inPlaceClosure
}

// within reports whether validating value costs at most limit applications.
// It walks the instance once, applying at each value every subschema the
// analysis says can reach it, then and else both included, so it can only
// overstate the work.
func (plan *workPlan) within(value any, limit int) bool {
	meter := &workMeter{closures: plan.closures, remaining: limit * workScale}
	return meter.visit(value, map[*fanoutNode]int{plan.root: 1})
}

type workMeter struct {
	closures  map[*fanoutNode]*inPlaceClosure
	remaining int
}

// visit charges the subschemas applied to value, in-place closures
// included, and then visits its members, items and property names.
func (meter *workMeter) visit(value any, applied map[*fanoutNode]int) bool {
	reached := map[*fanoutNode]int{}
	for node, count := range applied {
		closure, ok := meter.closures[node]
		if !ok || closure.saturated() {
			// Parse analysed every reachable node, so this cannot happen;
			// an unbounded node is refused rather than run.
			return false
		}
		for target, multiplicity := range closure.counts {
			reached[target] += count * multiplicity
		}
	}
	for target, count := range reached {
		meter.remaining -= count * target.cost(value)
		if meter.remaining < 0 {
			return false
		}
	}
	switch value := value.(type) {
	case map[string]any:
		for name, member := range value {
			children, names := map[*fanoutNode]int{}, map[*fanoutNode]int{}
			for target, count := range reached {
				// additionalProperties applies only where its schema lists
				// no such property.
				matched, ok := target.props[name]
				if !ok {
					matched = target.wildProps
				}
				for _, child := range matched {
					children[child] += count
				}
				for _, child := range target.names {
					names[child] += count
				}
			}
			if len(names) > 0 && !meter.visit(name, names) {
				return false
			}
			if len(children) > 0 && !meter.visit(member, children) {
				return false
			}
		}
	case []any:
		for index, item := range value {
			children := map[*fanoutNode]int{}
			for target, count := range reached {
				if index < len(target.prefix) {
					children[target.prefix[index]] += count
				} else if target.items != nil {
					children[target.items] += count
				}
			}
			if len(children) > 0 && !meter.visit(item, children) {
				return false
			}
		}
	}
	return true
}
