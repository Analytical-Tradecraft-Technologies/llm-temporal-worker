package kubernetes_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeploymentVerificationRejectsRootRenderedWorkload(t *testing.T) {
	t.Parallel()

	temporaryDirectory := t.TempDir()
	renderedManifest := filepath.Join(temporaryDirectory, "rendered.yaml")
	if err := os.WriteFile(renderedManifest, []byte(rootRenderedWorkload), 0o600); err != nil {
		t.Fatalf("write rendered manifest: %v", err)
	}

	fakeKubectl := filepath.Join(temporaryDirectory, "kubectl")
	if err := os.WriteFile(fakeKubectl, []byte("#!/bin/sh\ncat \"$RENDERED_MANIFEST\"\n"), 0o700); err != nil {
		t.Fatalf("write fake kubectl: %v", err)
	}

	command := exec.Command(filepath.Join(moduleRoot(t), "deploy", "verify.sh"))
	command.Dir = moduleRoot(t)
	command.Env = append(os.Environ(), "KUBECTL="+fakeKubectl, "RENDERED_MANIFEST="+renderedManifest)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("deploy verification accepted a root rendered workload:\n%s", output)
	}
	if !strings.Contains(string(output), "runAsUser must be the positive numeric UID 65532") {
		t.Fatalf("deploy verification failed for an unexpected reason:\n%s", output)
	}
}

func TestDeploymentPolicyTargetsPropagateKUBECTL(t *testing.T) {
	makefile := readRepositoryFile(t, "Makefile")
	for _, target := range []string{"deployment-policy-verify", "kustomize-verify"} {
		body := makeTarget(t, makefile, target)
		if !strings.Contains(body, `KUBECTL="$(KUBECTL)" $(GO) test ./integration/kubernetes`) {
			t.Fatalf("%s must propagate KUBECTL to the rendered Kubernetes policy test:\n%s", target, body)
		}
	}
}

func makeTarget(t *testing.T, makefile, name string) string {
	t.Helper()
	start := strings.Index(makefile, name+":\n")
	if start == -1 {
		t.Fatalf("Makefile is missing target %q", name)
	}
	body := makefile[start:]
	if end := strings.Index(body, "\n\n"); end != -1 {
		return body[:end]
	}
	return body
}

const rootRenderedWorkload = `apiVersion: v1
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
      securityContext:
        runAsNonRoot: true
        runAsUser: 0
        runAsGroup: 65532
        fsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      terminationGracePeriodSeconds: 120
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
