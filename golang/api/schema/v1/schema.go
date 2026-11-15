// Package schemav1 embeds the published v1 JSON Schemas, so a Go caller can
// read the exact schema revision of the module version it imports instead of
// cloning the repository. The contractkit package pairs these schemas with
// the shared v1 fixtures and a consumer runner (#1575).
package schemav1

import (
	"embed"
	"io/fs"
)

//go:embed *.schema.json
var files embed.FS

// FS returns the published v1 schemas, one *.schema.json file per contract
// at the root of the returned file system.
func FS() fs.FS { return files }
