package config

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const AuthorizationTrustedTemporal = "trusted_temporal"

// AuthorizationConfig explicitly trusts Temporal namespace callers to select
// any listed tenant/project pair. Temporal must enforce caller authentication
// and namespace access; an activity payload carries no authenticated identity.
// Omitting this policy is allowed for embeddings that supply their own resolver,
// but the production CLI requires it before constructing external clients.
type AuthorizationConfig struct {
	Mode          string            `yaml:"mode" json:"mode"`
	AllowedScopes []AuthorizedScope `yaml:"allowed_scopes" json:"allowed_scopes"`
}

type AuthorizedScope struct {
	Tenant  string `yaml:"tenant" json:"tenant"`
	Project string `yaml:"project" json:"project"`
}

// Validate rejects implicit trust, wildcard grants and duplicate scope pairs.
// Error messages identify configuration fields without echoing tenant names.
func (policy AuthorizationConfig) Validate() error {
	if policy.Mode != AuthorizationTrustedTemporal {
		return fmt.Errorf("authorization.mode must be trusted_temporal")
	}
	if len(policy.AllowedScopes) == 0 {
		return fmt.Errorf("authorization.allowed_scopes must not be empty")
	}
	seen := make(map[AuthorizedScope]struct{}, len(policy.AllowedScopes))
	for i, scope := range policy.AllowedScopes {
		for field, value := range map[string]string{"tenant": scope.Tenant, "project": scope.Project} {
			if !utf8.ValidString(value) || value == "" || len(value) > maxAdmissionFieldBytes || strings.ContainsAny(value, "*?") || strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
				return fmt.Errorf("authorization.allowed_scopes[%d].%s must be an exact identifier of at most %d bytes", i, field, maxAdmissionFieldBytes)
			}
		}
		if _, exists := seen[scope]; exists {
			return fmt.Errorf("authorization.allowed_scopes[%d] duplicates a scope", i)
		}
		seen[scope] = struct{}{}
	}
	return nil
}
