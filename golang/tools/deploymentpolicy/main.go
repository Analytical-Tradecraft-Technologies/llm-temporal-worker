// Command deploymentpolicy validates locally rendered Kubernetes workload
// manifests without contacting a cluster.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/netip"
	"os"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v4"
)

const runtimeIdentity = 65532

func main() {
	rendered := flag.String("rendered", "", "path to a rendered Kubernetes manifest")
	overlay := flag.String("overlay", "", "Kustomize overlay name")
	flag.Parse()

	if *rendered == "" || *overlay == "" {
		fmt.Fprintln(os.Stderr, "deployment policy verification requires --rendered and --overlay")
		os.Exit(2)
	}
	data, err := os.ReadFile(*rendered)
	if err != nil {
		fmt.Fprintln(os.Stderr, "deployment policy verification cannot read rendered manifest")
		os.Exit(1)
	}
	if err := verifyRendered(*overlay, data); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("deployment policy verified for %s\n", *overlay)
}

func verifyRendered(overlay string, rendered []byte) error {
	documents, err := decodeDocuments(rendered)
	if err != nil {
		return fmt.Errorf("deployment policy verification cannot decode %s: %w", overlay, err)
	}

	deployment, err := findResource(documents, "Deployment", "llmtw-worker")
	if err != nil {
		return fmt.Errorf("deployment policy verification %s: %w", overlay, err)
	}
	serviceAccount, err := findResource(documents, "ServiceAccount", "llmtw-worker")
	if err != nil {
		return fmt.Errorf("deployment policy verification %s: %w", overlay, err)
	}
	configMap, err := findResource(documents, "ConfigMap", "llmtw-config")
	if err != nil {
		return fmt.Errorf("deployment policy verification %s: %w", overlay, err)
	}
	networkPolicy, err := findResource(documents, "NetworkPolicy", "llmtw-worker")
	if err != nil {
		return fmt.Errorf("deployment policy verification %s: %w", overlay, err)
	}
	if err := verifyWorkload(overlay, deployment, serviceAccount, configMap); err != nil {
		return fmt.Errorf("deployment policy verification %s: %w", overlay, err)
	}
	if err := verifyNetworkPolicy(deployment, networkPolicy); err != nil {
		return fmt.Errorf("deployment policy verification %s: %w", overlay, err)
	}
	return nil
}

var stateControlPorts = map[int64]struct{}{
	5432: {}, // worker PostgreSQL state
	6379: {}, // Redis state and cache
	7233: {}, // Temporal frontend
}

func verifyNetworkPolicy(deployment, networkPolicy map[string]any) error {
	if err := verifyNetworkPolicyBinding(deployment, networkPolicy); err != nil {
		return err
	}
	spec, ok := mapAt(networkPolicy, "spec")
	if !ok {
		return errors.New("worker NetworkPolicy spec is missing")
	}
	egress, ok := listAt(spec, "egress")
	if !ok || len(egress) == 0 {
		return errors.New("worker NetworkPolicy egress rules are missing")
	}

	seenStateControlPorts := make(map[int64]bool, len(stateControlPorts))
	for _, rawRule := range egress {
		rule, ok := asMap(rawRule)
		if !ok {
			return errors.New("worker NetworkPolicy egress rule is invalid")
		}
		ports, portsDeclared := listAt(rule, "ports")
		if !portsDeclared || len(ports) == 0 {
			return errors.New("all-port egress must not use an unrestricted destination")
		}

		touchesStateControl := false
		touchesOtherPorts := false
		for _, rawPort := range ports {
			port, protocol, err := networkPolicyPort(rawPort)
			if err != nil {
				return err
			}
			if _, sensitive := stateControlPorts[port]; !sensitive {
				touchesOtherPorts = true
				continue
			}
			if protocol != "TCP" {
				return fmt.Errorf("state/control egress port %d must use TCP", port)
			}
			touchesStateControl = true
			seenStateControlPorts[port] = true
		}
		if !touchesStateControl {
			continue
		}
		if touchesOtherPorts {
			return errors.New("state/control egress must be separate from general TLS and other egress")
		}
		if err := verifyStateControlDestinations(rule); err != nil {
			return err
		}
	}

	for _, port := range []int64{5432, 6379, 7233} {
		if !seenStateControlPorts[port] {
			return fmt.Errorf("worker NetworkPolicy must declare reviewed state/control egress for TCP port %d", port)
		}
	}
	return nil
}

func verifyNetworkPolicyBinding(deployment, networkPolicy map[string]any) error {
	if resourceNamespace(deployment) != resourceNamespace(networkPolicy) {
		return errors.New("worker NetworkPolicy namespace must match the worker Deployment namespace")
	}

	deploymentSpec, ok := mapAt(deployment, "spec")
	if !ok {
		return errors.New("deployment spec is missing")
	}
	deploymentSelector, ok := mapAt(deploymentSpec, "selector")
	if !ok {
		return errors.New("worker Deployment selector is missing")
	}
	deploymentLabels, ok := exactMatchLabels(deploymentSelector)
	if !ok {
		return errors.New("worker Deployment selector must use explicit matchLabels")
	}
	template, ok := mapAt(deploymentSpec, "template")
	if !ok {
		return errors.New("deployment template is missing")
	}
	templateMetadata, ok := mapAt(template, "metadata")
	if !ok {
		return errors.New("worker Deployment template metadata is missing")
	}
	templateLabels, ok := mapAt(templateMetadata, "labels")
	if !ok || !containsStringLabels(templateLabels, deploymentLabels) {
		return errors.New("worker Deployment selector must match its pod-template labels")
	}

	networkPolicySpec, ok := mapAt(networkPolicy, "spec")
	if !ok {
		return errors.New("worker NetworkPolicy spec is missing")
	}
	policyTypes, ok := listAt(networkPolicySpec, "policyTypes")
	if !ok || !listContainsString(policyTypes, "Egress") {
		return errors.New("worker NetworkPolicy policyTypes must include Egress")
	}
	podSelector, ok := mapAt(networkPolicySpec, "podSelector")
	if !ok {
		return errors.New("worker NetworkPolicy podSelector is missing")
	}
	networkPolicyLabels, ok := exactMatchLabels(podSelector)
	if !ok || !equalStringLabels(networkPolicyLabels, deploymentLabels) {
		return errors.New("worker NetworkPolicy podSelector must exactly match the worker Deployment selector")
	}
	return nil
}

func resourceNamespace(resource map[string]any) string {
	metadata, ok := mapAt(resource, "metadata")
	if !ok {
		return "default"
	}
	namespace := stringAt(metadata, "namespace")
	if namespace == "" {
		return "default"
	}
	return namespace
}

func exactMatchLabels(selector map[string]any) (map[string]string, bool) {
	if len(selector) != 1 {
		return nil, false
	}
	labels, ok := mapAt(selector, "matchLabels")
	if !ok || len(labels) == 0 {
		return nil, false
	}
	result := make(map[string]string, len(labels))
	for key, value := range labels {
		text, ok := value.(string)
		if !ok || key == "" || text == "" {
			return nil, false
		}
		result[key] = text
	}
	return result, true
}

func containsStringLabels(values map[string]any, expected map[string]string) bool {
	for key, expectedValue := range expected {
		value, ok := values[key].(string)
		if !ok || value != expectedValue {
			return false
		}
	}
	return true
}

func equalStringLabels(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func networkPolicyPort(rawPort any) (int64, string, error) {
	entry, ok := asMap(rawPort)
	if !ok {
		return 0, "", errors.New("worker NetworkPolicy port entry is invalid")
	}
	if _, ranged := entry["endPort"]; ranged {
		return 0, "", errors.New("worker NetworkPolicy port ranges are not allowed")
	}
	port, ok := integerValue(valueAt(entry, "port"))
	if !ok || port < 1 || port > 65535 {
		return 0, "", errors.New("worker NetworkPolicy ports must be numeric values from 1 through 65535")
	}
	protocol := stringAt(entry, "protocol")
	if protocol == "" {
		protocol = "TCP"
	}
	return port, protocol, nil
}

func verifyStateControlDestinations(rule map[string]any) error {
	destinations, ok := listAt(rule, "to")
	if !ok || len(destinations) == 0 {
		return errors.New("state/control egress must declare explicit private or namespace destinations")
	}
	for _, rawDestination := range destinations {
		destination, ok := asMap(rawDestination)
		if !ok || len(destination) == 0 {
			return errors.New("state/control egress destination must be explicit")
		}
		if ipBlock, exists := destination["ipBlock"]; exists {
			block, ok := asMap(ipBlock)
			if !ok || len(destination) != 1 {
				return errors.New("state/control ipBlock destination is invalid")
			}
			cidr := stringAt(block, "cidr")
			if !isPrivateStateCIDR(cidr) {
				return fmt.Errorf("state/control egress CIDR must be private, got %q", cidr)
			}
			continue
		}

		rawNamespaceSelector, exists := destination["namespaceSelector"]
		if !exists {
			return errors.New("state/control selector destination must include llmtw.io/state-egress=allowed namespaceSelector")
		}
		namespaceSelector, ok := asMap(rawNamespaceSelector)
		if !ok || !stateEgressNamespaceSelector(namespaceSelector) {
			return errors.New("state/control namespaceSelector must require llmtw.io/state-egress=allowed")
		}

		selectorCount := 1
		if rawSelector, exists := destination["podSelector"]; exists {
			selectorCount++
			selector, ok := asMap(rawSelector)
			if !ok || !positivePodSelector(selector) {
				return errors.New("state/control podSelector must use positive matchLabels")
			}
		}
		if selectorCount == 0 || selectorCount != len(destination) {
			return errors.New("state/control egress destination must use an explicit ipBlock or label selector")
		}
	}
	return nil
}

func stateEgressNamespaceSelector(selector map[string]any) bool {
	labels, ok := exactMatchLabels(selector)
	return ok && len(labels) == 1 && labels["llmtw.io/state-egress"] == "allowed"
}

func positivePodSelector(selector map[string]any) bool {
	_, ok := exactMatchLabels(selector)
	return ok
}

func isPrivateStateCIDR(cidr string) bool {
	prefix, err := netip.ParsePrefix(strings.TrimSpace(cidr))
	if err != nil {
		return false
	}
	prefix = prefix.Masked()
	for _, privateCIDR := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7"} {
		privatePrefix := netip.MustParsePrefix(privateCIDR)
		if prefix.Addr().BitLen() == privatePrefix.Addr().BitLen() && prefix.Bits() >= privatePrefix.Bits() && privatePrefix.Contains(prefix.Addr()) {
			return true
		}
	}
	return false
}

func decodeDocuments(rendered []byte) ([]map[string]any, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(rendered))
	var documents []map[string]any
	for {
		var document map[string]any
		err := decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(document) != 0 {
			documents = append(documents, document)
		}
	}
	if len(documents) == 0 {
		return nil, errors.New("rendered manifest is empty")
	}
	return documents, nil
}

func findResource(documents []map[string]any, kind, name string) (map[string]any, error) {
	var found map[string]any
	for _, document := range documents {
		if stringAt(document, "kind") != kind {
			continue
		}
		metadata, ok := mapAt(document, "metadata")
		if !ok || stringAt(metadata, "name") != name {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("rendered %s %q appears more than once", strings.ToLower(kind), name)
		}
		found = document
	}
	if found != nil {
		return found, nil
	}
	return nil, fmt.Errorf("rendered %s %q is missing", strings.ToLower(kind), name)
}

func verifyWorkload(overlay string, deployment, serviceAccount, configMap map[string]any) error {
	podSpec, err := deploymentPodSpec(deployment)
	if err != nil {
		return err
	}
	securityContext, ok := mapAt(podSpec, "securityContext")
	if !ok {
		return errors.New("pod securityContext is missing")
	}
	if boolAt(securityContext, "runAsNonRoot") != true {
		return errors.New("runAsNonRoot must be true")
	}
	if !integerEqual(valueAt(securityContext, "runAsUser"), runtimeIdentity) {
		return errors.New("runAsUser must be the positive numeric UID 65532")
	}
	if !integerEqual(valueAt(securityContext, "runAsGroup"), runtimeIdentity) {
		return errors.New("runAsGroup must be the numeric GID 65532")
	}
	if !integerEqual(valueAt(securityContext, "fsGroup"), runtimeIdentity) {
		return errors.New("fsGroup must be the numeric fsGroup 65532")
	}
	seccomp, ok := mapAt(securityContext, "seccompProfile")
	if !ok || stringAt(seccomp, "type") != "RuntimeDefault" {
		return errors.New("seccompProfile.type must be RuntimeDefault")
	}
	if !positiveInteger(valueAt(podSpec, "terminationGracePeriodSeconds")) {
		return errors.New("terminationGracePeriodSeconds must be a positive number")
	}
	if err := verifyShutdownGrace(podSpec, configMap); err != nil {
		return err
	}
	if initContainers, exists := podSpec["initContainers"]; exists {
		entries, ok := initContainers.([]any)
		if !ok || len(entries) != 0 {
			return errors.New("init containers are not allowed")
		}
	}

	if err := verifyServiceAccountPolicy(overlay, podSpec, serviceAccount); err != nil {
		return err
	}
	containers, ok := listAt(podSpec, "containers")
	if !ok || len(containers) != 1 {
		return errors.New("worker deployment must contain exactly one container")
	}
	container, err := namedListItem(podSpec, "containers", "worker")
	if err != nil {
		return err
	}
	if err := verifyWorkerContainer(container); err != nil {
		return err
	}
	if err := verifyWritableMounts(container, podSpec); err != nil {
		return err
	}
	return nil
}

// verifyShutdownGrace binds the rendered Kubernetes grace period to the
// process shutdown budget mounted in the same ConfigMap. A positive grace
// value by itself is insufficient: if it expires at the same time as the
// worker's shutdown deadline, Kubernetes can send SIGKILL before clients and
// telemetry finish closing. The strict inequality is the deployment-level
// margin required by the runtime shutdown contract.
func verifyShutdownGrace(podSpec, configMap map[string]any) error {
	data, ok := mapAt(configMap, "data")
	if !ok {
		return errors.New("llmtw-config ConfigMap data is missing")
	}
	encoded := stringAt(data, "config.yaml")
	if strings.TrimSpace(encoded) == "" {
		return errors.New("llmtw-config ConfigMap must include config.yaml")
	}
	var document struct {
		Server struct {
			ShutdownTimeout string `yaml:"shutdown_timeout"`
		} `yaml:"server"`
	}
	if err := yaml.Unmarshal([]byte(encoded), &document); err != nil {
		return errors.New("llmtw-config shutdown budget is not valid YAML")
	}
	shutdown, err := time.ParseDuration(strings.TrimSpace(document.Server.ShutdownTimeout))
	if err != nil || shutdown <= 0 {
		return errors.New("llmtw-config server.shutdown_timeout must be a positive duration")
	}
	graceSeconds, ok := integerValue(valueAt(podSpec, "terminationGracePeriodSeconds"))
	if !ok || graceSeconds <= 0 || graceSeconds > math.MaxInt64/int64(time.Second) {
		return errors.New("terminationGracePeriodSeconds must be a bounded integer")
	}
	grace := time.Duration(graceSeconds) * time.Second
	if grace <= shutdown {
		return fmt.Errorf("terminationGracePeriodSeconds (%s) must exceed server.shutdown_timeout (%s)", grace, shutdown)
	}
	return nil
}

func deploymentPodSpec(deployment map[string]any) (map[string]any, error) {
	spec, ok := mapAt(deployment, "spec")
	if !ok {
		return nil, errors.New("deployment spec is missing")
	}
	template, ok := mapAt(spec, "template")
	if !ok {
		return nil, errors.New("deployment template is missing")
	}
	podSpec, ok := mapAt(template, "spec")
	if !ok {
		return nil, errors.New("deployment pod spec is missing")
	}
	return podSpec, nil
}

func verifyServiceAccountPolicy(overlay string, podSpec, serviceAccount map[string]any) error {
	if stringAt(podSpec, "serviceAccountName") != "llmtw-worker" {
		return errors.New("worker must use the llmtw-worker service account")
	}
	podAutomount, podAutomountSet := boolValue(valueAt(podSpec, "automountServiceAccountToken"))
	accountAutomount, accountAutomountSet := boolValue(valueAt(serviceAccount, "automountServiceAccountToken"))
	if !podAutomountSet || !accountAutomountSet || podAutomount != accountAutomount {
		return errors.New("pod and service account must declare the same token automount policy")
	}

	switch overlay {
	case "aws-workload-identity":
		if !podAutomount || !serviceAccountAnnotation(serviceAccount, "eks.amazonaws.com/role-arn") {
			return errors.New("AWS workload identity must opt in to the reviewed service account token policy")
		}
	case "azure-workload-identity":
		if !podAutomount || !serviceAccountAnnotation(serviceAccount, "azure.workload.identity/client-id") {
			return errors.New("Azure workload identity must opt in to the reviewed service account token policy")
		}
	default:
		if podAutomount {
			return errors.New("service account token automount is allowed only in a workload identity overlay")
		}
	}
	return nil
}

func serviceAccountAnnotation(serviceAccount map[string]any, key string) bool {
	metadata, ok := mapAt(serviceAccount, "metadata")
	if !ok {
		return false
	}
	annotations, ok := mapAt(metadata, "annotations")
	if !ok {
		return false
	}
	return stringAt(annotations, key) != ""
}

func verifyWorkerContainer(container map[string]any) error {
	if !strings.Contains(stringAt(container, "image"), "@sha256:") {
		return errors.New("worker image must be digest pinned")
	}
	securityContext, ok := mapAt(container, "securityContext")
	if !ok {
		return errors.New("worker securityContext is missing")
	}
	if err := rejectContainerSecurityOverrides(securityContext); err != nil {
		return err
	}
	allowPrivilegeEscalation, allowPrivilegeEscalationSet := boolValue(valueAt(securityContext, "allowPrivilegeEscalation"))
	if !allowPrivilegeEscalationSet || allowPrivilegeEscalation {
		return errors.New("worker must disable privilege escalation")
	}
	if boolAt(securityContext, "readOnlyRootFilesystem") != true {
		return errors.New("worker root filesystem must be read-only")
	}
	capabilities, ok := mapAt(securityContext, "capabilities")
	if !ok || !listContainsString(valueAt(capabilities, "drop"), "ALL") {
		return errors.New("worker must drop every Linux capability")
	}
	if err := verifyResources(container); err != nil {
		return err
	}
	if err := verifyHealthPort(container); err != nil {
		return err
	}
	if err := verifyProbe(container, "livenessProbe", "/health/live"); err != nil {
		return err
	}
	if err := verifyProbe(container, "readinessProbe", "/health/ready"); err != nil {
		return err
	}
	return nil
}

func rejectContainerSecurityOverrides(securityContext map[string]any) error {
	for _, field := range []string{
		"runAsUser",
		"runAsGroup",
		"runAsNonRoot",
		"privileged",
		"seccompProfile",
	} {
		if _, exists := securityContext[field]; exists {
			return fmt.Errorf("container securityContext must not set %s", field)
		}
	}
	if capabilities, ok := mapAt(securityContext, "capabilities"); ok {
		if _, exists := capabilities["add"]; exists {
			return errors.New("worker must not add Linux capabilities")
		}
	}
	return nil
}

func verifyResources(container map[string]any) error {
	resources, ok := mapAt(container, "resources")
	if !ok {
		return errors.New("worker resource constraints are missing")
	}
	for _, class := range []string{"requests", "limits"} {
		values, ok := mapAt(resources, class)
		if !ok || stringAt(values, "cpu") == "" || stringAt(values, "memory") == "" {
			return fmt.Errorf("worker %s must bound CPU and memory", class)
		}
	}
	return nil
}

func verifyHealthPort(container map[string]any) error {
	ports, ok := listAt(container, "ports")
	if !ok {
		return errors.New("worker health port is missing")
	}
	for _, port := range ports {
		entry, ok := asMap(port)
		if ok && stringAt(entry, "name") == "health" && integerEqual(valueAt(entry, "containerPort"), 8080) {
			return nil
		}
	}
	return errors.New("worker must expose a named health port")
}

func verifyProbe(container map[string]any, name, path string) error {
	probe, ok := mapAt(container, name)
	if !ok {
		return fmt.Errorf("worker %s is missing", name)
	}
	httpGet, ok := mapAt(probe, "httpGet")
	if !ok || stringAt(httpGet, "path") != path || !healthPortReference(valueAt(httpGet, "port")) {
		return fmt.Errorf("worker %s must use the health port and %s", name, path)
	}
	return nil
}

func healthPortReference(value any) bool {
	name, ok := value.(string)
	return ok && name == "health"
}

func verifyWritableMounts(container, podSpec map[string]any) error {
	mounts, ok := listAt(container, "volumeMounts")
	if !ok {
		return errors.New("worker volume mounts are missing")
	}
	volumes, ok := listAt(podSpec, "volumes")
	if !ok {
		return errors.New("pod volumes are missing")
	}
	volumeByName := make(map[string]map[string]any, len(volumes))
	for _, volume := range volumes {
		entry, ok := asMap(volume)
		if !ok {
			return errors.New("pod volume is invalid")
		}
		name := stringAt(entry, "name")
		if name == "" {
			return errors.New("pod volume name is missing")
		}
		if _, unsafe := entry["hostPath"]; unsafe {
			return errors.New("hostPath volumes are not allowed")
		}
		volumeByName[name] = entry
	}

	writableMounts := 0
	for _, mount := range mounts {
		entry, ok := asMap(mount)
		if !ok {
			return errors.New("worker volume mount is invalid")
		}
		name := stringAt(entry, "name")
		path := stringAt(entry, "mountPath")
		if name == "" || path == "" || volumeByName[name] == nil {
			return errors.New("worker volume mount must reference a declared volume")
		}
		readOnly, set := boolValue(valueAt(entry, "readOnly"))
		if path != "/tmp" {
			if !set || !readOnly {
				return fmt.Errorf("worker mount %s must be read-only", path)
			}
			continue
		}
		if set && readOnly {
			return errors.New("worker /tmp mount must be writable")
		}
		writableMounts++
		if err := verifyBoundedTemporaryVolume(volumeByName[name]); err != nil {
			return err
		}
	}
	if writableMounts != 1 {
		return errors.New("worker must have exactly one writable /tmp mount")
	}
	return nil
}

func verifyBoundedTemporaryVolume(volume map[string]any) error {
	emptyDir, ok := mapAt(volume, "emptyDir")
	if !ok || stringAt(emptyDir, "medium") != "Memory" || stringAt(emptyDir, "sizeLimit") == "" {
		return errors.New("worker /tmp must use a bounded memory emptyDir")
	}
	return nil
}

func namedListItem(parent map[string]any, key, name string) (map[string]any, error) {
	entries, ok := listAt(parent, key)
	if !ok {
		return nil, fmt.Errorf("%s is missing", key)
	}
	for _, entry := range entries {
		value, ok := asMap(entry)
		if ok && stringAt(value, "name") == name {
			return value, nil
		}
	}
	return nil, fmt.Errorf("%s %q is missing", key, name)
}

func mapAt(value map[string]any, key string) (map[string]any, bool) {
	return asMap(valueAt(value, key))
}

func listAt(value map[string]any, key string) ([]any, bool) {
	list, ok := valueAt(value, key).([]any)
	return list, ok
}

func asMap(value any) (map[string]any, bool) {
	mapping, ok := value.(map[string]any)
	return mapping, ok
}

func valueAt(value map[string]any, key string) any {
	return value[key]
}

func stringAt(value map[string]any, key string) string {
	text, _ := valueAt(value, key).(string)
	return text
}

func boolAt(value map[string]any, key string) bool {
	result, _ := boolValue(valueAt(value, key))
	return result
}

func boolValue(value any) (bool, bool) {
	result, ok := value.(bool)
	return result, ok
}

func positiveInteger(value any) bool {
	switch number := value.(type) {
	case int:
		return number > 0
	case int8:
		return number > 0
	case int16:
		return number > 0
	case int32:
		return number > 0
	case int64:
		return number > 0
	case uint:
		return number > 0
	case uint8:
		return number > 0
	case uint16:
		return number > 0
	case uint32:
		return number > 0
	case uint64:
		return number > 0
	case float32:
		return number > 0 && math.Trunc(float64(number)) == float64(number)
	case float64:
		return number > 0 && math.Trunc(number) == number
	default:
		return false
	}
}

func integerEqual(value any, expected int) bool {
	switch number := value.(type) {
	case int:
		return number == expected
	case int8:
		return int(number) == expected
	case int16:
		return int(number) == expected
	case int32:
		return int(number) == expected
	case int64:
		return number == int64(expected)
	case uint:
		return number == uint(expected)
	case uint8:
		return int(number) == expected
	case uint16:
		return int(number) == expected
	case uint32:
		return number == uint32(expected)
	case uint64:
		return number == uint64(expected)
	case float32:
		return math.Trunc(float64(number)) == float64(number) && int(number) == expected
	case float64:
		return math.Trunc(number) == number && int(number) == expected
	default:
		return false
	}
}

func integerValue(value any) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int8:
		return int64(number), true
	case int16:
		return int64(number), true
	case int32:
		return int64(number), true
	case int64:
		return number, true
	case uint:
		if uint64(number) > math.MaxInt64 {
			return 0, false
		}
		return int64(number), true
	case uint8:
		return int64(number), true
	case uint16:
		return int64(number), true
	case uint32:
		return int64(number), true
	case uint64:
		if number > math.MaxInt64 {
			return 0, false
		}
		return int64(number), true
	case float32:
		if math.Trunc(float64(number)) != float64(number) || float64(number) > math.MaxInt64 || float64(number) < math.MinInt64 {
			return 0, false
		}
		return int64(number), true
	case float64:
		if math.Trunc(number) != number || number > math.MaxInt64 || number < math.MinInt64 {
			return 0, false
		}
		return int64(number), true
	default:
		return 0, false
	}
}

func listContainsString(value any, expected string) bool {
	values, ok := value.([]any)
	if !ok {
		return false
	}
	for _, value := range values {
		text, ok := value.(string)
		if ok && text == expected {
			return true
		}
	}
	return false
}
