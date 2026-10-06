package runtime

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func TestRuntimePassesVersionIdentityToSDKWorker(t *testing.T) {
	var closed atomic.Bool
	options := testRuntimeOptions(t, &testWorker{}, &closed)
	var actual worker.DeploymentOptions
	var registered *testRegistry
	options.WorkerFactory = func(_ client.Client, _ string, value worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
		actual = value.DeploymentOptions
		registered = &testRegistry{}
		return &testWorker{}, registered, nil
	}
	data := strings.Replace(string(runtimeConfig(t)), "  worker:\n", "  worker:\n    versioning: {enabled: true, deployment_name: worker, build_id: build-1}\n", 1)
	value, err := New(context.Background(), []byte(data), options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = value.Shutdown(context.Background()) }()
	if actual != (worker.DeploymentOptions{UseVersioning: true, Version: worker.WorkerDeploymentVersion{DeploymentName: "worker", BuildID: "build-1"}, DefaultVersioningBehavior: workflow.VersioningBehaviorPinned}) {
		t.Fatalf("SDK versioning options: %+v", actual)
	}
	if len(registered.workflows) == 0 {
		t.Fatal("versioned runtime did not register workflows")
	}
}
