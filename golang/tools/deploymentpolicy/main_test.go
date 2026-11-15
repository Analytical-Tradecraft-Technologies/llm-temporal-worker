package main

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"strings"
	"testing"
)

func TestVerifyRenderedAcceptsBaseWorkloadPolicy(t *testing.T) {
	testassert.NoError(t, verifyRendered("base", []byte(validRenderedWorkload)), "verify rendered base workload")
}

func TestVerifyRenderedRejectsPublicStateAndControlEgress(t *testing.T) {
	tests := []struct {
		name string
		cidr string
	}{
		{name: "IPv4 default route", cidr: "0.0.0.0/0"},
		{name: "IPv6 default route", cidr: "::/0"},
		{name: "public network", cidr: "198.51.100.0/24"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workload := strings.Replace(validRenderedWorkload, "cidr: 10.64.0.0/16", "cidr: "+test.cidr, 1)
			assertRenderedBaseRejected(t, workload, "state/control egress CIDR must be private", "public state/control egress")
		})
	}
}

func TestVerifyRenderedRejectsUnscopedStateAndControlSelectors(t *testing.T) {
	workload := strings.Replace(validRenderedWorkload, "namespaceSelector:\n            matchLabels:\n              llmtw.io/state-egress: allowed", "namespaceSelector: {}", 1)
	assertRenderedBaseRejected(t, workload, "state/control namespaceSelector must require llmtw.io/state-egress=allowed", "unscoped state/control selector")
}

func TestVerifyRenderedRequiresSeparateTLSAndStateControlRules(t *testing.T) {
	workload := strings.Replace(validRenderedWorkload, "        - port: 7233\n          protocol: TCP", "        - port: 7233\n          protocol: TCP\n        - port: 443\n          protocol: TCP", 1)
	assertRenderedBaseRejected(t, workload, "state/control egress must be separate from general TLS", "mixed TLS and state/control egress")
}

func TestVerifyRenderedRejectsUnrestrictedAllPortEgress(t *testing.T) {
	workload := strings.Replace(validRenderedWorkload, "  egress:\n", "  egress:\n    - to:\n        - ipBlock:\n            cidr: 0.0.0.0/0\n", 1)
	assertRenderedBaseRejected(t, workload, "all-port egress must not use an unrestricted destination", "unrestricted all-port egress")
}

func TestVerifyRenderedRejectsPortRangesThatSpanStateControlPorts(t *testing.T) {
	workload := strings.Replace(validRenderedWorkload, "        - port: 443\n          protocol: TCP", "        - port: 443\n          endPort: 8000\n          protocol: TCP", 1)
	assertRenderedBaseRejected(t, workload, "NetworkPolicy port ranges are not allowed", "ranged egress")
}

func TestVerifyRenderedBindsNetworkPolicyToWorkerDeployment(t *testing.T) {
	assertRenderedChangesRejected(t, "worker NetworkPolicy binding", []renderedChange{
		{name: "selector targets another workload", change: replaceOnce("podSelector:\n    matchLabels:\n      app.kubernetes.io/name: llm-temporal-worker\n      app.kubernetes.io/component: worker", "podSelector:\n    matchLabels:\n      app.kubernetes.io/name: another-worker\n      app.kubernetes.io/component: worker"), message: "podSelector must exactly match the worker Deployment selector"},
		{name: "selector targets every pod", change: replaceOnce("podSelector:\n    matchLabels:\n      app.kubernetes.io/name: llm-temporal-worker\n      app.kubernetes.io/component: worker", "podSelector: {}"), message: "podSelector must exactly match the worker Deployment selector"},
		{
			name: "different namespace",
			change: func(workload string) string {
				marker := "kind: NetworkPolicy\nmetadata:\n  name: llmtw-worker\n  namespace: llmtw-system"
				return strings.Replace(workload, marker, strings.Replace(marker, "llmtw-system", "another-namespace", 1), 1)
			},
			message: "NetworkPolicy namespace must match the worker Deployment namespace",
		},
		{name: "egress policy type omitted", change: replaceOnce("policyTypes: [Ingress, Egress]", "policyTypes: [Ingress]"), message: "NetworkPolicy policyTypes must include Egress"},
	})
}

func TestVerifyRenderedRejectsNegativeStateNamespaceSelectors(t *testing.T) {
	tests := []struct {
		name       string
		expression string
	}{
		{
			name: "DoesNotExist",
			expression: `matchExpressions:
              - key: llmtw.io/state-egress
                operator: DoesNotExist`,
		},
		{
			name: "NotIn",
			expression: `matchExpressions:
              - key: llmtw.io/state-egress
                operator: NotIn
                values: [allowed]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workload := strings.Replace(validRenderedWorkload, "matchLabels:\n              llmtw.io/state-egress: allowed", test.expression, 1)
			assertRenderedBaseRejected(t, workload, "state/control namespaceSelector must require llmtw.io/state-egress=allowed", "negative namespace selector")
		})
	}
}

func TestVerifyRenderedRejectsPodSelectorOnlyStateDestination(t *testing.T) {
	workload := strings.Replace(validRenderedWorkload, `namespaceSelector:
            matchLabels:
              llmtw.io/state-egress: allowed`, `podSelector:
            matchLabels:
              app.kubernetes.io/name: redis`, 1)
	assertRenderedBaseRejected(t, workload, "state/control selector destination must include llmtw.io/state-egress=allowed namespaceSelector", "pod-selector-only destination")
}

func TestVerifyRenderedRejectsInsufficientShutdownGrace(t *testing.T) {
	workload := strings.Replace(validRenderedWorkload, "terminationGracePeriodSeconds: 120", "terminationGracePeriodSeconds: 90", 1)
	testassert.ErrorContains(t, verifyRendered("base", []byte(workload)), "must exceed server.shutdown_timeout")
}

func TestVerifyRenderedRejectsDuplicateRequiredResources(t *testing.T) {
	for _, test := range []struct {
		name      string
		duplicate string
		resource  string
	}{
		{
			name: "deployment",
			duplicate: `apiVersion: apps/v1
kind: Deployment
metadata:
  name: llmtw-worker
`,
			resource: "deployment",
		},
		{
			name: "service account",
			duplicate: `apiVersion: v1
kind: ServiceAccount
metadata:
  name: llmtw-worker
`,
			resource: "serviceaccount",
		},
		{
			name: "config map",
			duplicate: `apiVersion: v1
kind: ConfigMap
metadata:
  name: llmtw-config-5f8c2d7b9h
`,
			resource: "configmap",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rendered := validRenderedWorkload + "---\n" + test.duplicate
			err := verifyRendered("base", []byte(rendered))
			want := "rendered " + test.resource + " \"llmtw-"
			if err == nil || !strings.Contains(err.Error(), "appears more than once") || !strings.Contains(err.Error(), want) {
				t.Fatalf("verify rendered duplicate %s error = %v, want duplicate-resource rejection", test.name, err)
			}
		})
	}
}

func TestVerifyRenderedRejectsWorkloadPolicyViolations(t *testing.T) {
	assertRenderedChangesRejected(t, "base workload", []renderedChange{
		{name: "root UID", change: replaceOnce("runAsUser: 65532", "runAsUser: 0"), message: "positive numeric UID"},
		{name: "string UID", change: replaceOnce("runAsUser: 65532", "runAsUser: \"65532\""), message: "positive numeric UID"},
		{name: "container UID override", change: replaceOnce("securityContext:\n            allowPrivilegeEscalation: false", "securityContext:\n            runAsUser: 65532\n            allowPrivilegeEscalation: false"), message: "container securityContext must not set runAsUser"},
		{name: "container group override", change: replaceOnce("securityContext:\n            allowPrivilegeEscalation: false", "securityContext:\n            runAsGroup: 65532\n            allowPrivilegeEscalation: false"), message: "container securityContext must not set runAsGroup"},
		{name: "container non-root override", change: replaceOnce("securityContext:\n            allowPrivilegeEscalation: false", "securityContext:\n            runAsNonRoot: true\n            allowPrivilegeEscalation: false"), message: "container securityContext must not set runAsNonRoot"},
		{name: "container privileged override", change: replaceOnce("securityContext:\n            allowPrivilegeEscalation: false", "securityContext:\n            privileged: true\n            allowPrivilegeEscalation: false"), message: "container securityContext must not set privileged"},
		{name: "container capability addition", change: replaceOnce("drop: [ALL]", "drop: [ALL]\n              add: [NET_ADMIN]"), message: "worker must not add Linux capabilities"},
		{name: "container seccomp override", change: replaceOnce("securityContext:\n            allowPrivilegeEscalation: false", "securityContext:\n            seccompProfile:\n              type: RuntimeDefault\n            allowPrivilegeEscalation: false"), message: "container securityContext must not set seccompProfile"},
		{name: "unexpected group", change: replaceOnce("runAsGroup: 65532", "runAsGroup: 1"), message: "numeric GID 65532"},
		{name: "missing file-system group", change: replaceOnce("        fsGroup: 65532\n", ""), message: "numeric fsGroup 65532"},
		{name: "writable root", change: replaceOnce("readOnlyRootFilesystem: true", "readOnlyRootFilesystem: false"), message: "root filesystem"},
		{name: "unbounded temporary storage", change: replaceOnce("\n            sizeLimit: 128Mi", ""), message: "bounded memory emptyDir"},
		{name: "additional writable mount", change: replaceOnce("mountPath: /etc/llmtw\n              readOnly: true", "mountPath: /etc/llmtw\n              readOnly: false"), message: "must be read-only"},
		{name: "unbounded resources", change: replaceOnce("\n            limits:\n              cpu: \"2\"\n              memory: 2Gi", ""), message: "limits must bound CPU and memory"},
		{name: "wrong readiness endpoint", change: replaceOnce("path: /health/ready", "path: /health/live"), message: "/health/ready"},
		{name: "wrong health container port", change: replaceOnce("containerPort: 8080", "containerPort: 80"), message: "named health port"},
		{name: "wrong liveness port reference", change: replaceOnce("path: /health/live\n              port: health", "path: /health/live\n              port: 9999"), message: "health port"},
		{name: "unapproved service account token", change: replaceAll("automountServiceAccountToken: false", "automountServiceAccountToken: true"), message: "only in a workload identity overlay"},
		{name: "init container", change: replaceOnce("      containers:\n", "      initContainers:\n        - name: unsafe\n          image: busybox@sha256:REPLACE_WITH_RELEASE_DIGEST\n      containers:\n"), message: "init containers are not allowed"},
		{name: "additional container", change: replaceOnce("      volumes:\n", "        - name: sidecar\n          image: busybox@sha256:REPLACE_WITH_RELEASE_DIGEST\n      volumes:\n"), message: "exactly one container"},
	})
}

func TestVerifyRenderedAllowsOnlyAnnotatedIdentityOverlays(t *testing.T) {
	awsWorkload := strings.ReplaceAll(validRenderedWorkload, "automountServiceAccountToken: false", "automountServiceAccountToken: true")
	awsWorkload = strings.Replace(awsWorkload, "metadata:\n  name: llmtw-worker\nautomount", "metadata:\n  name: llmtw-worker\n  annotations:\n    eks.amazonaws.com/role-arn: arn:aws:iam::REPLACE_ACCOUNT:role/REPLACE_ROLE\nautomount", 1)
	testassert.NoError(t, verifyRendered("aws-workload-identity", []byte(awsWorkload)), "verify rendered AWS workload identity")

	testassert.ErrorContains(t, verifyRendered("azure-workload-identity", []byte(awsWorkload)), "Azure workload identity")
}

const validRenderedWorkload = `apiVersion: v1
kind: ServiceAccount
metadata:
  name: llmtw-worker
automountServiceAccountToken: false
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: llmtw-worker
  namespace: llmtw-system
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: llm-temporal-worker
      app.kubernetes.io/component: worker
  template:
    metadata:
      labels:
        app.kubernetes.io/name: llm-temporal-worker
        app.kubernetes.io/component: worker
    spec:
      serviceAccountName: llmtw-worker
      automountServiceAccountToken: false
      terminationGracePeriodSeconds: 120
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: worker
          image: docker.io/analyticaltradecraft/llm-temporal-worker@sha256:REPLACE_WITH_RELEASE_DIGEST
          ports:
            - name: health
              containerPort: 8080
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: [ALL]
          resources:
            requests:
              cpu: 250m
              memory: 512Mi
            limits:
              cpu: "2"
              memory: 2Gi
          livenessProbe:
            httpGet:
              path: /health/live
              port: health
          readinessProbe:
            httpGet:
              path: /health/ready
              port: health
          volumeMounts:
            - name: config
              mountPath: /etc/llmtw
              readOnly: true
            - name: runtime-secrets
              mountPath: /var/run/secrets/llmtw
              readOnly: true
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: config
          configMap:
            name: llmtw-config-5f8c2d7b9h
        - name: runtime-secrets
          secret:
            secretName: llmtw-worker-secrets
        - name: tmp
          emptyDir:
            medium: Memory
            sizeLimit: 128Mi
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: llmtw-worker
  namespace: llmtw-system
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: llm-temporal-worker
      app.kubernetes.io/component: worker
  policyTypes: [Ingress, Egress]
  egress:
    - ports:
        - port: 53
          protocol: UDP
        - port: 53
          protocol: TCP
      to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
    - ports:
        - port: 6379
          protocol: TCP
        - port: 5432
          protocol: TCP
        - port: 7233
          protocol: TCP
      to:
        - namespaceSelector:
            matchLabels:
              llmtw.io/state-egress: allowed
        - ipBlock:
            cidr: 10.64.0.0/16
    - ports:
        - port: 443
          protocol: TCP
      to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - 169.254.169.254/32
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: llmtw-config-5f8c2d7b9h
immutable: true
data:
  config.yaml: |
    server:
      shutdown_timeout: 90s
    service_classes: [economy, standard, priority]
`

func TestVerifyRenderedRequiresContentHashedImmutableConfigMap(t *testing.T) {
	assertRenderedChangesRejected(t, "base workload", []renderedChange{
		{name: "fixed name", change: replaceAll("llmtw-config-5f8c2d7b9h", "llmtw-config"), message: "content-hashed"},
		{name: "static suffix", change: replaceAll("llmtw-config-5f8c2d7b9h", "llmtw-config-static"), message: "content-hashed"},
		{name: "mutable", change: replaceOnce("immutable: true\n", ""), message: "must be immutable"},
	})
}

// More than one replica must be spread across nodes (#1515).
func TestVerifyRenderedRequiresReplicaSpread(t *testing.T) {
	withReplicas := strings.Replace(validRenderedWorkload, "  namespace: llmtw-system\nspec:\n  selector:", "  namespace: llmtw-system\nspec:\n  replicas: 2\n  selector:", 1)
	if withReplicas == validRenderedWorkload {
		t.Fatal("fixture no longer has the expected deployment spec")
	}
	const anchor = "      terminationGracePeriodSeconds: 120\n"
	spread := func(rule string) string {
		return strings.Replace(withReplicas, anchor, anchor+rule, 1)
	}
	const workerLabels = "              app.kubernetes.io/name: llm-temporal-worker\n              app.kubernetes.io/component: worker\n"
	for name, test := range map[string]struct {
		workload string
		ok       bool
	}{
		"no spread":                               {workload: withReplicas},
		"hostname spread":                         {workload: spread("      topologySpreadConstraints:\n        - maxSkew: 1\n          topologyKey: kubernetes.io/hostname\n          whenUnsatisfiable: ScheduleAnyway\n          labelSelector:\n            matchLabels:\n" + workerLabels), ok: true},
		"zone spread only":                        {workload: spread("      topologySpreadConstraints:\n        - maxSkew: 1\n          topologyKey: topology.kubernetes.io/zone\n          whenUnsatisfiable: ScheduleAnyway\n          labelSelector:\n            matchLabels:\n" + workerLabels)},
		"spread selecting other pods":             {workload: spread("      topologySpreadConstraints:\n        - maxSkew: 1\n          topologyKey: kubernetes.io/hostname\n          whenUnsatisfiable: ScheduleAnyway\n          labelSelector:\n            matchLabels:\n              app.kubernetes.io/name: something-else\n")},
		"spread with a label the pods lack":       {workload: spread("      topologySpreadConstraints:\n        - maxSkew: 1\n          topologyKey: kubernetes.io/hostname\n          whenUnsatisfiable: ScheduleAnyway\n          labelSelector:\n            matchLabels:\n" + workerLabels + "              app.kubernetes.io/instance: other\n")},
		"spread with match expressions":           {workload: spread("      topologySpreadConstraints:\n        - maxSkew: 1\n          topologyKey: kubernetes.io/hostname\n          whenUnsatisfiable: ScheduleAnyway\n          labelSelector:\n            matchLabels:\n" + workerLabels + "            matchExpressions:\n              - key: app.kubernetes.io/name\n                operator: NotIn\n                values: [llm-temporal-worker]\n")},
		"anti-affinity in another namespace":      {workload: spread("      affinity:\n        podAntiAffinity:\n          preferredDuringSchedulingIgnoredDuringExecution:\n            - weight: 100\n              podAffinityTerm:\n                topologyKey: kubernetes.io/hostname\n                namespaces: [other-namespace]\n                labelSelector:\n                  matchLabels:\n                    app.kubernetes.io/name: llm-temporal-worker\n                    app.kubernetes.io/component: worker\n")},
		"anti-affinity with a namespace selector": {workload: spread("      affinity:\n        podAntiAffinity:\n          requiredDuringSchedulingIgnoredDuringExecution:\n            - topologyKey: kubernetes.io/hostname\n              namespaceSelector:\n                matchLabels:\n                  team: other\n              labelSelector:\n                matchLabels:\n                  app.kubernetes.io/name: llm-temporal-worker\n                  app.kubernetes.io/component: worker\n")},
		"anti-affinity with mismatch label keys":  {workload: spread("      affinity:\n        podAntiAffinity:\n          preferredDuringSchedulingIgnoredDuringExecution:\n            - weight: 100\n              podAffinityTerm:\n                topologyKey: kubernetes.io/hostname\n                mismatchLabelKeys: [pod-template-hash]\n                labelSelector:\n                  matchLabels:\n                    app.kubernetes.io/name: llm-temporal-worker\n                    app.kubernetes.io/component: worker\n")},
		"spread with match label keys":            {workload: spread("      topologySpreadConstraints:\n        - maxSkew: 1\n          topologyKey: kubernetes.io/hostname\n          whenUnsatisfiable: ScheduleAnyway\n          matchLabelKeys: [pod-template-hash]\n          labelSelector:\n            matchLabels:\n" + workerLabels), ok: true},
		"preferred anti-affinity":                 {workload: spread("      affinity:\n        podAntiAffinity:\n          preferredDuringSchedulingIgnoredDuringExecution:\n            - weight: 100\n              podAffinityTerm:\n                topologyKey: kubernetes.io/hostname\n                labelSelector:\n                  matchLabels:\n                    app.kubernetes.io/name: llm-temporal-worker\n                    app.kubernetes.io/component: worker\n"), ok: true},
	} {
		t.Run(name, func(t *testing.T) {
			err := verifyRendered("base", []byte(test.workload))
			if test.ok && err != nil {
				t.Fatalf("verify rendered = %v", err)
			}
			if !test.ok && (err == nil || !strings.Contains(err.Error(), "spread them across nodes")) {
				t.Fatalf("verify rendered = %v, want a replica spread error", err)
			}
		})
	}
}

// replaceOnce returns a change that replaces the first old with new.
func replaceOnce(old, new string) func(string) string {
	return func(value string) string {
		return strings.Replace(value, old, new, 1)
	}
}

// replaceAll returns a change that replaces every old with new.
func replaceAll(old, new string) func(string) string {
	return func(value string) string {
		return strings.ReplaceAll(value, old, new)
	}
}

// renderedChange is a named edit of the valid rendered workload that the base
// overlay must reject with an error containing message.
type renderedChange struct {
	name    string
	change  func(string) string
	message string
}

// assertRenderedChangesRejected runs each change as a subtest; label names
// the violations.
func assertRenderedChangesRejected(t *testing.T, label string, changes []renderedChange) {
	t.Helper()
	for _, test := range changes {
		t.Run(test.name, func(t *testing.T) {
			assertRenderedBaseRejected(t, test.change(validRenderedWorkload), test.message, label)
		})
	}
}

// assertRenderedBaseRejected requires verifyRendered to reject workload as a
// base overlay with an error containing want; label names the violation.
func assertRenderedBaseRejected(t *testing.T, workload, want, label string) {
	t.Helper()
	err := verifyRendered("base", []byte(workload))
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("verify rendered %s error = %v, want %q", label, err, want)
	}
}
