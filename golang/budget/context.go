package budget

import (
	"errors"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

// ErrContextLimit excludes a candidate whose model cannot fit the estimated
// input and reserved output/reasoning. Other estimator failures are hard errors.
var ErrContextLimit = errors.New("request exceeds model context token limit")

// ValidateContext performs no pricing, admission, or provider I/O. It uses the
// configured tokenizer, or the same approximate input estimate as reservation.
// A zero catalog limit means unspecified, not an unlimited provider window.
func (estimator Estimator) ValidateContext(request llm.Request, candidate routing.Candidate) error {
	if candidate.ContextTokens == 0 {
		return nil
	}
	resolved, err := llm.NormalizeRequest(request)
	if err != nil {
		return err
	}
	resolved.Model, resolved.ServiceClass = candidate.Model, candidate.AttemptedClass
	resolved.ServiceClassFallbacks = nil
	input, err := estimator.CountInputTokens(resolved, candidate)
	if err != nil {
		return err
	}
	output, err := estimator.outputLimit(resolved)
	if err != nil {
		return err
	}
	reasoning := estimator.MaxReasoning
	if resolved.Reasoning != nil && resolved.Reasoning.TokenBudget != nil && int64(*resolved.Reasoning.TokenBudget) > reasoning {
		reasoning = int64(*resolved.Reasoning.TokenBudget)
	}
	return validateContextCounts(candidate.ContextTokens, input, output, reasoning)
}

func validateContextCounts(limit, input, output, reasoning int64) error {
	if limit < 0 || input < 0 || output < 0 || reasoning < 0 {
		return fmt.Errorf("context limit and token counts must not be negative")
	}
	if limit == 0 {
		return nil
	}
	// Subtraction avoids overflow even for adversarial int64 token counts.
	remaining := limit
	for _, count := range []int64{input, output, reasoning} {
		if count > remaining {
			return ErrContextLimit
		}
		remaining -= count
	}
	return nil
}
