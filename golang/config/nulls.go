package config

import (
	"fmt"
	"reflect"
	"strings"

	yaml "go.yaml.in/yaml/v4"
)

var yamlUnmarshalerType = reflect.TypeOf((*yaml.Unmarshaler)(nil)).Elem()

// rejectNulls reports the first explicit YAML null (~, null, or an empty
// value) written where the decoder would silently substitute a value: a
// scalar or section (`require_match: ~` would mean false) or a list element
// (`allowed_tenants: [~]`). A null for a whole list, map or optional pointer
// setting is the same as omitting it and stays accepted, which also keeps the
// rendered effective configuration loadable. Free-form extension values are
// not inspected. data must already have decoded into out, whose type drives
// the walk.
func rejectNulls(data []byte, out any, path string) error {
	var document yaml.Node
	if err := yaml.Load(data, &document, yaml.WithV4Defaults()); err != nil {
		return fmt.Errorf("configuration YAML: %w", err)
	}
	node := &document
	if node.Kind == yaml.DocumentNode && len(node.Content) == 1 {
		node = node.Content[0]
	}
	return rejectNullNode(node, reflect.TypeOf(out).Elem(), path, 0)
}

func rejectNullNode(node *yaml.Node, target reflect.Type, path string, depth int) error {
	// Aliases let a small document expand into a deep walk; the decoder has
	// already bounded the document, so a generous depth guard is enough here.
	if node == nil || depth > 128 {
		return nil
	}
	if node.Kind == yaml.AliasNode {
		return rejectNullNode(node.Alias, target, path, depth+1)
	}
	if target.Kind() == reflect.Interface {
		return nil
	}
	if target.Kind() == reflect.Pointer {
		if isNullNode(node) {
			return nil
		}
		return rejectNullNode(node, target.Elem(), path, depth+1)
	}
	if isNullNode(node) {
		if target.Kind() == reflect.Map || target.Kind() == reflect.Slice {
			return nil
		}
		if path == "" {
			path = "configuration"
		}
		return fmt.Errorf("%s must not be null", path)
	}
	if reflect.PointerTo(target).Implements(yamlUnmarshalerType) {
		return nil
	}
	switch {
	case target.Kind() == reflect.Struct && node.Kind == yaml.MappingNode:
		fields := yamlFields(target)
		for index := 0; index+1 < len(node.Content); index += 2 {
			key, value := node.Content[index], node.Content[index+1]
			if key.ShortTag() == "!!merge" {
				merged := []*yaml.Node{value}
				if value.Kind == yaml.SequenceNode {
					merged = value.Content
				}
				for _, source := range merged {
					if err := rejectNullNode(source, target, path, depth+1); err != nil {
						return err
					}
				}
				continue
			}
			if field, ok := fields[key.Value]; ok {
				if err := rejectNullNode(value, field, joinFieldPath(path, key.Value), depth+1); err != nil {
					return err
				}
			}
		}
	case target.Kind() == reflect.Map && node.Kind == yaml.MappingNode:
		for index := 0; index+1 < len(node.Content); index += 2 {
			if err := rejectNullNode(node.Content[index+1], target.Elem(), joinFieldPath(path, node.Content[index].Value), depth+1); err != nil {
				return err
			}
		}
	case (target.Kind() == reflect.Slice || target.Kind() == reflect.Array) && node.Kind == yaml.SequenceNode:
		for index, element := range node.Content {
			if element.Kind == yaml.AliasNode {
				element = element.Alias
			}
			if isNullNode(element) && target.Elem().Kind() != reflect.Interface {
				return fmt.Errorf("%s[%d] must not be null", path, index)
			}
			if err := rejectNullNode(element, target.Elem(), fmt.Sprintf("%s[%d]", path, index), depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func isNullNode(node *yaml.Node) bool {
	return node.Kind == yaml.ScalarNode && node.ShortTag() == "!!null"
}

func yamlFields(target reflect.Type) map[string]reflect.Type {
	fields := make(map[string]reflect.Type, target.NumField())
	for index := 0; index < target.NumField(); index++ {
		field := target.Field(index)
		if !field.IsExported() {
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = strings.ToLower(field.Name)
		}
		fields[name] = field.Type
	}
	return fields
}

func joinFieldPath(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}
