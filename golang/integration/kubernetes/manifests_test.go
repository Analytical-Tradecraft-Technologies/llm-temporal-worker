package kubernetes_test

import (
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v4"
)

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	directory := filepath.Dir(source)
	for {
		if _, err := os.Stat(filepath.Join(directory, ".git")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("repository checkout root not found")
		}
		directory = parent
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(repositoryRoot(t), "golang")
}

func readRepositoryFile(t *testing.T, path ...string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(append([]string{moduleRoot(t)}, path...)...))
	if err != nil {
		t.Fatalf("read %s: %v", filepath.Join(path...), err)
	}
	return string(data)
}

func TestBaseManifestSecurityContract(t *testing.T) {
	deployment := readRepositoryFile(t, "deploy", "kubernetes", "base", "deployment.yaml")
	for _, marker := range []string{
		"runAsNonRoot: true",
		"readOnlyRootFilesystem: true",
		"drop: [ALL]",
		"type: RuntimeDefault",
		"terminationGracePeriodSeconds: 120",
		"path: /health/live",
		"path: /health/ready",
		"@sha256:",
	} {
		if !strings.Contains(deployment, marker) {
			t.Errorf("base deployment is missing %q", marker)
		}
	}
	if strings.Contains(deployment, "image: ") && regexp.MustCompile(`(?m)^\s*image:\s*[^\s@]+:[^\s@]+\s*$`).MatchString(deployment) {
		t.Error("base deployment must not use a mutable image tag")
	}
	if strings.Contains(deployment, "name: LLMTW_REDIS_KEY_PREFIX") {
		t.Error("base deployment must not override Redis key_prefix from the ConfigMap")
	}
	config := readRepositoryFile(t, "deploy", "kubernetes", "base", "config.yaml")
	if !strings.Contains(config, "key_prefix: llmtw") {
		t.Error("base ConfigMap configuration must declare the Redis key prefix")
	}

	serviceAccount := readRepositoryFile(t, "deploy", "kubernetes", "base", "serviceaccount.yaml")
	if !strings.Contains(serviceAccount, "automountServiceAccountToken: false") {
		t.Error("base service account must disable token automount")
	}

	kustomization := readRepositoryFile(t, "deploy", "kubernetes", "base", "kustomization.yaml")
	for _, resource := range []string{"deployment.yaml", "service.yaml", "networkpolicy.yaml", "poddisruptionbudget.yaml"} {
		if !strings.Contains(kustomization, resource) {
			t.Errorf("base kustomization is missing %s", resource)
		}
	}
}

func TestEveryOverlayReferencesBaseAndUsesAReviewablePatch(t *testing.T) {
	overlays := map[string][]string{
		"redis-tls":               {"deployment-patch.yaml"},
		"private-state-egress":    {"networkpolicy-patch.yaml"},
		"aws-workload-identity":   {"deployment-patch.yaml", "serviceaccount-patch.yaml"},
		"azure-workload-identity": {"deployment-patch.yaml", "serviceaccount-patch.yaml"},
	}
	for overlay, patches := range overlays {
		directory := []string{"deploy", "kubernetes", "examples", overlay}
		kustomization := readRepositoryFile(t, append(directory, "kustomization.yaml")...)
		if !strings.Contains(kustomization, "../../base") {
			t.Errorf("%s overlay does not reference ../../base", overlay)
		}
		for _, patch := range patches {
			if !strings.Contains(kustomization, patch) {
				t.Errorf("%s overlay does not declare patch %s", overlay, patch)
			}
			if _, err := os.Stat(filepath.Join(append([]string{moduleRoot(t)}, append(directory, patch)...)...)); err != nil {
				t.Errorf("%s overlay patch %s is missing: %v", overlay, patch, err)
			}
		}
	}
}

func TestBaseNetworkPolicyScopesStateAndControlEgress(t *testing.T) {
	var policy struct {
		Spec struct {
			Egress []struct {
				Ports []struct {
					Port     int    `yaml:"port"`
					Protocol string `yaml:"protocol"`
				} `yaml:"ports"`
				To []struct {
					NamespaceSelector struct {
						MatchLabels map[string]string `yaml:"matchLabels"`
					} `yaml:"namespaceSelector"`
					IPBlock struct {
						CIDR string `yaml:"cidr"`
					} `yaml:"ipBlock"`
				} `yaml:"to"`
			} `yaml:"egress"`
		} `yaml:"spec"`
	}
	policyData := readRepositoryFile(t, "deploy", "kubernetes", "base", "networkpolicy.yaml")
	if err := yaml.Unmarshal([]byte(policyData), &policy); err != nil {
		t.Fatalf("decode base NetworkPolicy: %v", err)
	}
	config := readRepositoryFile(t, "deploy", "kubernetes", "base", "config.yaml")

	if !strings.Contains(config, "addresses: [postgres.example.internal:5432]") {
		t.Fatal("base configuration must use the worker PostgreSQL endpoint on port 5432")
	}

	sensitive := map[int]bool{5432: true, 6379: true, 7233: true}
	seen := make(map[int]bool, len(sensitive))
	for _, rule := range policy.Spec.Egress {
		if len(rule.Ports) == 0 {
			t.Fatal("base NetworkPolicy must not declare all-port egress")
		}
		touchesStateControl := false
		touchesOther := false
		for _, port := range rule.Ports {
			if !sensitive[port.Port] {
				touchesOther = true
				continue
			}
			touchesStateControl = true
			seen[port.Port] = true
			if port.Protocol != "TCP" {
				t.Errorf("state/control egress port %d protocol = %q, want TCP", port.Port, port.Protocol)
			}
		}
		if !touchesStateControl {
			continue
		}
		if touchesOther {
			t.Error("base NetworkPolicy must keep state/control ports separate from general egress")
		}
		if len(rule.To) == 0 {
			t.Fatal("state/control egress must declare destinations")
		}
		for _, destination := range rule.To {
			if len(destination.NamespaceSelector.MatchLabels) > 0 {
				continue
			}
			if !privateNetwork(destination.IPBlock.CIDR) {
				t.Errorf("state/control destination %q is neither an explicit namespace selector nor a private CIDR", destination.IPBlock.CIDR)
			}
		}
	}
	for port := range sensitive {
		if !seen[port] {
			t.Errorf("base NetworkPolicy is missing reviewed state/control egress for TCP %d", port)
		}
	}
}

func privateNetwork(cidr string) bool {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		return false
	}
	prefix = prefix.Masked()
	for _, allowed := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7"} {
		privatePrefix := netip.MustParsePrefix(allowed)
		if prefix.Addr().BitLen() == privatePrefix.Addr().BitLen() && prefix.Bits() >= privatePrefix.Bits() && privatePrefix.Contains(prefix.Addr()) {
			return true
		}
	}
	return false
}
