// Package cache contains provider-neutral exact-response cache identities and
// persistence contracts. Storage adapters implement ResponseRepository.
package cache

import "fmt"

// These named strings keep route identity fields from being accidentally
// interchanged at call sites. Empty account and region values are valid for
// providers that do not expose either dimension, but are still encoded into
// the identity so two routes cannot silently share a key.
type Provider string
type Endpoint string
type Account string
type Region string
type Model string
type ModelRevision string
type CompilerProfile string
type ConfigDigest string
type CapabilityVersion string
type CacheEpoch string
type ConversationDigest string
type ProviderStateDigest string

// RouteIdentity is the resolved provider route used for a call. Display or
// logical model aliases must not be used in place of the resolved revision.
type RouteIdentity struct {
	Provider Provider        `json:"provider"`
	Endpoint Endpoint        `json:"endpoint"`
	Account  Account         `json:"account"`
	Region   Region          `json:"region"`
	Model    Model           `json:"model"`
	Revision ModelRevision   `json:"revision"`
	Compiler CompilerProfile `json:"compiler"`
}

func (identity RouteIdentity) validate() error {
	for name, value := range map[string]string{
		"provider": string(identity.Provider),
		"endpoint": string(identity.Endpoint),
		"model":    string(identity.Model),
		"revision": string(identity.Revision),
		"compiler": string(identity.Compiler),
	} {
		if value == "" {
			return fmt.Errorf("route identity %s is required", name)
		}
	}
	return nil
}
