package main

import (
	"strings"
	"testing"
)

func TestVerifyRenderedAcceptsBaseWorkloadPolicy(t *testing.T) {
	if err := verifyRendered("base", []byte(validRenderedWorkload)); err != nil {
		t.Fatalf("verify rendered base workload: %v", err)
	}
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
			err := verifyRendered("base", []byte(workload))
			if err == nil || !strings.Contains(err.Error(), "state/control egress CIDR must be private") {
				t.Fatalf("verify rendered public state/control egress error = %v, want private-CIDR rejection", err)
			}
		})
	}
}

func TestVerifyRenderedRejectsUnscopedStateAndControlSelectors(t *testing.T) {
	workload := strings.Replace(validRenderedWorkload, "namespaceSelector:\n            matchLabels:\n              llmtw.io/state-egress: allowed", "namespaceSelector: {}", 1)
	err := verifyRendered("base", []byte(workload))
	if err == nil || !strings.Contains(err.Error(), "state/control namespaceSelector must require llmtw.io/state-egress=allowed") {
		t.Fatalf("verify rendered unscoped state/control selector error = %v, want explicit-selector rejection", err)
	}
}

func TestVerifyRenderedRequiresSeparateTLSAndStateControlRules(t *testing.T) {
	workload := strings.Replace(validRenderedWorkload, "        - port: 7233\n          protocol: TCP", "        - port: 7233\n          protocol: TCP\n        - port: 443\n          protocol: TCP", 1)
	err := verifyRendered("base", []byte(workload))
	if err == nil || !strings.Contains(err.Error(), "state/control egress must be separate from general TLS") {
		t.Fatalf("verify rendered mixed TLS and state/control egress error = %v, want rule-separation rejection", err)
	}
}

func TestVerifyRenderedRejectsUnrestrictedAllPortEgress(t *testing.T) {
	workload := strings.Replace(validRenderedWorkload, "  egress:\n", "  egress:\n    - to:\n        - ipBlock:\n            cidr: 0.0.0.0/0\n", 1)
	err := verifyRendered("base", []byte(workload))
	if err == nil || !strings.Contains(err.Error(), "all-port egress must not use an unrestricted destination") {
		t.Fatalf("verify rendered unrestricted all-port egress error = %v, want unrestricted-destination rejection", err)
	}
}

func TestVerifyRenderedRejectsPortRangesThatSpanStateControlPorts(t *testing.T) {
	workload := strings.Replace(validRenderedWorkload, "        - port: 443\n          protocol: TCP", "        - port: 443\n          endPort: 8000\n          protocol: TCP", 1)
	err := verifyRendered("base", []byte(workload))
	if err == nil || !strings.Contains(err.Error(), "NetworkPolicy port ranges are not allowed") {
		t.Fatalf("verify rendered ranged egress error = %v, want port-range rejection", err)
	}
}

func TestVerifyRenderedBindsNetworkPolicyToWorkerDeployment(t *testing.T) {
	tests := []struct {
		name    string
		change  func(string) string
		message string
	}{
		{
			name: "selector targets another workload",
			change: func(workload string) string {
				return strings.Replace(workload, "podSelector:\n    matchLabels:\n      app.kubernetes.io/name: llm-temporal-worker\n      app.kubernetes.io/component: worker", "podSelector:\n    matchLabels:\n      app.kubernetes.io/name: another-worker\n      app.kubernetes.io/component: worker", 1)
			},
			message: "podSelector must exactly match the worker Deployment selector",
		},
		{
			name: "selector targets every pod",
			change: func(workload string) string {
				return strings.Replace(workload, "podSelector:\n    matchLabels:\n      app.kubernetes.io/name: llm-temporal-worker\n      app.kubernetes.io/component: worker", "podSelector: {}", 1)
			},
			message: "podSelector must exactly match the worker Deployment selector",
		},
		{
			name: "different namespace",
			change: func(workload string) string {
				marker := "kind: NetworkPolicy\nmetadata:\n  name: llmtw-worker\n  namespace: llmtw-system"
				return strings.Replace(workload, marker, strings.Replace(marker, "llmtw-system", "another-namespace", 1), 1)
			},
			message: "NetworkPolicy namespace must match the worker Deployment namespace",
		},
		{
			name: "egress policy type omitted",
			change: func(workload string) string {
				return strings.Replace(workload, "policyTypes: [Ingress, Egress]", "policyTypes: [Ingress]", 1)
			},
			message: "NetworkPolicy policyTypes must include Egress",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := verifyRendered("base", []byte(test.change(validRenderedWorkload)))
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("verify rendered worker NetworkPolicy binding error = %v, want %q", err, test.message)
			}
		})
	}
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
			err := verifyRendered("base", []byte(workload))
			if err == nil || !strings.Contains(err.Error(), "state/control namespaceSelector must require llmtw.io/state-egress=allowed") {
				t.Fatalf("verify rendered negative namespace selector error = %v, want positive opt-in rejection", err)
			}
		})
	}
}

func TestVerifyRenderedRejectsPodSelectorOnlyStateDestination(t *testing.T) {
	workload := strings.Replace(validRenderedWorkload, `namespaceSelector:
            matchLabels:
              llmtw.io/state-egress: allowed`, `podSelector:
            matchLabels:
              app.kubernetes.io/name: redis`, 1)
	err := verifyRendered("base", []byte(workload))
	if err == nil || !strings.Contains(err.Error(), "state/control selector destination must include llmtw.io/state-egress=allowed namespaceSelector") {
		t.Fatalf("verify rendered pod-selector-only destination error = %v, want required namespace opt-in rejection", err)
	}
}

func TestVerifyRenderedRejectsInsufficientShutdownGrace(t *testing.T) {
	workload := strings.Replace(validRenderedWorkload, "terminationGracePeriodSeconds: 120", "terminationGracePeriodSeconds: 90", 1)
	if err := verifyRendered("base", []byte(workload)); err == nil || !strings.Contains(err.Error(), "must exceed server.shutdown_timeout") {
		t.Fatalf("verify rendered shutdown grace error = %v, want insufficient shutdown grace error", err)
	}
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
  name: llmtw-config
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
	tests := []struct {
		name    string
		change  func(string) string
		message string
	}{
		{
			name: "root UID",
			change: func(value string) string {
				return strings.Replace(value, "runAsUser: 65532", "runAsUser: 0", 1)
			},
			message: "positive numeric UID",
		},
		{
			name: "string UID",
			change: func(value string) string {
				return strings.Replace(value, "runAsUser: 65532", "runAsUser: \"65532\"", 1)
			},
			message: "positive numeric UID",
		},
		{
			name: "container UID override",
			change: func(value string) string {
				return strings.Replace(value, "securityContext:\n            allowPrivilegeEscalation: false", "securityContext:\n            runAsUser: 65532\n            allowPrivilegeEscalation: false", 1)
			},
			message: "container securityContext must not set runAsUser",
		},
		{
			name: "container group override",
			change: func(value string) string {
				return strings.Replace(value, "securityContext:\n            allowPrivilegeEscalation: false", "securityContext:\n            runAsGroup: 65532\n            allowPrivilegeEscalation: false", 1)
			},
			message: "container securityContext must not set runAsGroup",
		},
		{
			name: "container non-root override",
			change: func(value string) string {
				return strings.Replace(value, "securityContext:\n            allowPrivilegeEscalation: false", "securityContext:\n            runAsNonRoot: true\n            allowPrivilegeEscalation: false", 1)
			},
			message: "container securityContext must not set runAsNonRoot",
		},
		{
			name: "container privileged override",
			change: func(value string) string {
				return strings.Replace(value, "securityContext:\n            allowPrivilegeEscalation: false", "securityContext:\n            privileged: true\n            allowPrivilegeEscalation: false", 1)
			},
			message: "container securityContext must not set privileged",
		},
		{
			name: "container capability addition",
			change: func(value string) string {
				return strings.Replace(value, "drop: [ALL]", "drop: [ALL]\n              add: [NET_ADMIN]", 1)
			},
			message: "worker must not add Linux capabilities",
		},
		{
			name: "container seccomp override",
			change: func(value string) string {
				return strings.Replace(value, "securityContext:\n            allowPrivilegeEscalation: false", "securityContext:\n            seccompProfile:\n              type: RuntimeDefault\n            allowPrivilegeEscalation: false", 1)
			},
			message: "container securityContext must not set seccompProfile",
		},
		{
			name: "unexpected group",
			change: func(value string) string {
				return strings.Replace(value, "runAsGroup: 65532", "runAsGroup: 1", 1)
			},
			message: "numeric GID 65532",
		},
		{
			name: "missing file-system group",
			change: func(value string) string {
				return strings.Replace(value, "        fsGroup: 65532\n", "", 1)
			},
			message: "numeric fsGroup 65532",
		},
		{
			name: "writable root",
			change: func(value string) string {
				return strings.Replace(value, "readOnlyRootFilesystem: true", "readOnlyRootFilesystem: false", 1)
			},
			message: "root filesystem",
		},
		{
			name: "unbounded temporary storage",
			change: func(value string) string {
				return strings.Replace(value, "\n            sizeLimit: 128Mi", "", 1)
			},
			message: "bounded memory emptyDir",
		},
		{
			name: "additional writable mount",
			change: func(value string) string {
				return strings.Replace(value, "mountPath: /etc/llmtw\n              readOnly: true", "mountPath: /etc/llmtw\n              readOnly: false", 1)
			},
			message: "must be read-only",
		},
		{
			name: "unbounded resources",
			change: func(value string) string {
				return strings.Replace(value, "\n            limits:\n              cpu: \"2\"\n              memory: 2Gi", "", 1)
			},
			message: "limits must bound CPU and memory",
		},
		{
			name: "wrong readiness endpoint",
			change: func(value string) string {
				return strings.Replace(value, "path: /health/ready", "path: /health/live", 1)
			},
			message: "/health/ready",
		},
		{
			name: "wrong health container port",
			change: func(value string) string {
				return strings.Replace(value, "containerPort: 8080", "containerPort: 80", 1)
			},
			message: "named health port",
		},
		{
			name: "wrong liveness port reference",
			change: func(value string) string {
				return strings.Replace(value, "path: /health/live\n              port: health", "path: /health/live\n              port: 9999", 1)
			},
			message: "health port",
		},
		{
			name: "unapproved service account token",
			change: func(value string) string {
				return strings.Replace(value, "automountServiceAccountToken: false", "automountServiceAccountToken: true", -1)
			},
			message: "only in a workload identity overlay",
		},
		{
			name: "init container",
			change: func(value string) string {
				return strings.Replace(value, "      containers:\n", "      initContainers:\n        - name: unsafe\n          image: busybox@sha256:REPLACE_WITH_RELEASE_DIGEST\n      containers:\n", 1)
			},
			message: "init containers are not allowed",
		},
		{
			name: "additional container",
			change: func(value string) string {
				return strings.Replace(value, "      volumes:\n", "        - name: sidecar\n          image: busybox@sha256:REPLACE_WITH_RELEASE_DIGEST\n      volumes:\n", 1)
			},
			message: "exactly one container",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := verifyRendered("base", []byte(test.change(validRenderedWorkload)))
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("verify rendered base workload error = %v, want %q", err, test.message)
			}
		})
	}
}

func TestVerifyRenderedAllowsOnlyAnnotatedIdentityOverlays(t *testing.T) {
	awsWorkload := strings.ReplaceAll(validRenderedWorkload, "automountServiceAccountToken: false", "automountServiceAccountToken: true")
	awsWorkload = strings.Replace(awsWorkload, "metadata:\n  name: llmtw-worker\nautomount", "metadata:\n  name: llmtw-worker\n  annotations:\n    eks.amazonaws.com/role-arn: arn:aws:iam::REPLACE_ACCOUNT:role/REPLACE_ROLE\nautomount", 1)
	if err := verifyRendered("aws-workload-identity", []byte(awsWorkload)); err != nil {
		t.Fatalf("verify rendered AWS workload identity: %v", err)
	}

	if err := verifyRendered("azure-workload-identity", []byte(awsWorkload)); err == nil || !strings.Contains(err.Error(), "Azure workload identity") {
		t.Fatalf("verify rendered Azure workload identity error = %v", err)
	}
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
          image: ghcr.io/mfow/llm-temporal-worker@sha256:REPLACE_WITH_RELEASE_DIGEST
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
            name: llmtw-config
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
  name: llmtw-config
data:
  config.yaml: |
    server:
      shutdown_timeout: 90s
    service_classes: [economy, standard, priority]
`
