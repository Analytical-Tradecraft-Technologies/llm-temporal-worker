package bedrockconverse

import (
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestProfileAcceptsUndeclaredServiceClassesAsUnsupported(t *testing.T) {
	profile := DefaultProfile("standard-only")
	profile.ServiceTiers = map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: ""}
	validated, err := NewProfile(profile)
	if err != nil {
		t.Fatalf("standard-only profile rejected: %v", err)
	}
	if tier, err := validated.providerTier(llm.ServiceClassStandard); err != nil || tier != "default" {
		t.Fatalf("standard tier = %q, %v", tier, err)
	}
	if _, err := validated.providerTier(llm.ServiceClassEconomy); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("economy tier error = %v, want unsupported", err)
	}
	profile.ServiceTiers = map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "", llm.ServiceClassPriority: ""}
	if _, err := NewProfile(profile); err == nil || !strings.Contains(err.Error(), "at least one service class") {
		t.Fatalf("profile with no supported class error = %v", err)
	}
}
