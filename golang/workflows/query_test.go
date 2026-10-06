package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"testing"
)

func TestPublicQueryWorkflow(t *testing.T) {
	for _, mode := range []string{"success", "mismatch", "failure", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			var input llm.QueryRequestV1
			workflowJSON(t, "query-provider-status", &input)
			var response llm.QueryResponseV1
			workflowJSON(t, "query-provider-response", &response)
			response.OperationKey = input.OperationKey
			calls := 0
			env.RegisterWorkflowWithOptions(rawWorkflow(activity.PayloadLimits{}, Query), workflow.RegisterOptions{Name: QueryWorkflowName})
			env.RegisterActivityWithOptions(func(context.Context, llm.QueryRequestV1) (*llm.QueryResponseV1, error) {
				calls++
				if mode == "failure" {
					return nil, errors.New("transient query failure")
				}
				if mode == "mismatch" {
					response.OperationKey = "wrong-operation"
				}
				return &response, nil
			}, sdkactivity.RegisterOptions{Name: activity.QueryActivityName})
			if mode == "invalid" {
				input.OperationKey = ""
			}
			if mode == "invalid" {
				env.ExecuteWorkflow(QueryWorkflowName, json.RawMessage(`{"api_version":"llm.temporal/query/v1","operation_key":"","context":{"tenant":"acme","project":"test","actor":"actor"},"kind":"provider_status","query":{}}`))
			} else {
				env.ExecuteWorkflow(QueryWorkflowName, input)
			}
			if mode == "success" {
				if err := env.GetWorkflowError(); err != nil {
					t.Fatal(err)
				}
				var got llm.QueryResponseV1
				if err := env.GetWorkflowResult(&got); err != nil || got.OperationKey != input.OperationKey {
					t.Fatalf("result=%#v error=%v", got, err)
				}
			} else if env.GetWorkflowError() == nil {
				t.Fatal("accepted invalid query result")
			}
			expected := 1
			if mode == "invalid" {
				expected = 0
			}
			if calls != expected {
				t.Fatalf("activity attempts=%d want %d", calls, expected)
			}
		})
	}
}
