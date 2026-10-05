// Package buildinfo exposes the immutable metadata stamped into a worker
// binary at build time. It deliberately contains no configuration or runtime
// credentials, so it is safe to expose through the version command.
package buildinfo

import "runtime"

const defaultSource = "https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker"

var (
	Version   = "dev"
	Revision  = "unknown"
	BuildTime = "unknown"
	Source    = defaultSource
	GoVersion string
)

// Metadata is the five-field contract shared by the final OCI image and the
// binary's version command.
type Metadata struct {
	Version   string `json:"version"`
	Revision  string `json:"revision"`
	BuildTime string `json:"build_time"`
	GoVersion string `json:"go_version"`
	Source    string `json:"source"`
}

// Current returns the metadata embedded into this binary. Direct developer
// builds do not supply a linker Go-version value, so they report the compiler
// runtime version rather than an empty field.
func Current() Metadata {
	goVersion := GoVersion
	if goVersion == "" {
		goVersion = runtime.Version()
	}
	return Metadata{
		Version:   Version,
		Revision:  Revision,
		BuildTime: BuildTime,
		GoVersion: goVersion,
		Source:    Source,
	}
}
