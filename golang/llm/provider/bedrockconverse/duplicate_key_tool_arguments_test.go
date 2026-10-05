package bedrockconverse

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// rawDocument is a tool input document whose encoded form is fixed, so the
// lift sees exactly the bytes a provider document would marshal to.
type rawDocument struct {
	document.Interface
	raw string
}

func (d rawDocument) MarshalSmithyDocument() ([]byte, error) { return []byte(d.raw), nil }

func TestDuplicateKeyToolInputIsAnAcceptedInvalidResponse(t *testing.T) {
	adapter, err := New(&Client{converse: &fakeConverse{}}, "endpoint", DefaultProfile("nova"))
	if err != nil {
		t.Fatal(err)
	}
	for raw, valid := range map[string]bool{`{"q":"a","q":"b"}`: false, `{"query":"x","limit":5}`: true} {
		output := &types.ConverseOutputMemberMessage{Value: types.Message{Content: []types.ContentBlock{
			&types.ContentBlockMemberToolUse{Value: types.ToolUseBlock{ToolUseId: aws.String("call"), Name: aws.String("lookup"), Input: rawDocument{raw: raw}}},
		}}}
		_, err := adapter.liftResponse(provider.Call{}, &bedrockruntime.ConverseOutput{Output: output, StopReason: types.StopReasonToolUse, ServiceTier: &types.ServiceTier{Type: types.ServiceTierTypeDefault}}, "request-id")
		if valid {
			if err != nil {
				t.Fatalf("control tool input %s rejected: %v", raw, err)
			}
			continue
		}
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Phase != provider.PhaseLift || mapped.Dispatch != provider.DispatchAccepted {
			t.Fatalf("duplicate-key tool input error = %#v", err)
		}
	}
}
