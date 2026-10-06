package app_test

import (
	domainactivity "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"
	"reflect"
	"testing"
	"time"
)

func TestWorkerVersionIdentityAndPinnedBehaviorSurviveDependencyPause(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "unversioned", true: "versioned"}[enabled], func(t *testing.T) {
			var captured []worker.DeploymentOptions
			version := config.WorkerVersioningConfig{}
			expected := worker.DeploymentOptions{}
			if enabled {
				version = config.WorkerVersioningConfig{Enabled: true, DeploymentName: "worker", BuildID: "build-1"}
				expected = worker.DeploymentOptions{UseVersioning: true, Version: worker.WorkerDeploymentVersion{DeploymentName: "worker", BuildID: "build-1"}, DefaultVersioningBehavior: sdkworkflow.VersioningBehaviorPinned}
			}
			value, err := app.NewWorker(app.WorkerOptions{
				Versioning: version, TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1, GracefulStopTimeout: time.Second,
				Activities: &domainactivity.Activities{},
				Factory: func(_ client.Client, _ string, options worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
					captured = append(captured, options.DeploymentOptions)
					return &fakeWorker{}, &fakeRegistry{}, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer value.Stop()
			if err := value.Start(); err != nil {
				t.Fatal(err)
			}
			value.Pause()
			deadline := time.Now().Add(time.Second)
			for {
				err = value.Resume()
				if err == nil {
					break
				}
				if err != app.ErrWorkerDraining || time.Now().After(deadline) {
					t.Fatal(err)
				}
				time.Sleep(time.Millisecond)
			}
			if !reflect.DeepEqual(captured, []worker.DeploymentOptions{expected, expected}) {
				t.Fatalf("version identity changed after pause: %+v", captured)
			}
		})
	}
}
