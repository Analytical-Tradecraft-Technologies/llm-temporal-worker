package budget

import (
	"errors"
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

// ErrContextLimit excludes a candidate whose model cannot fit the estimated
// input and the output cap. Other estimator failures are hard errors.
var ErrContextLimit = errors.New("request exceeds model context token limit")

// ValidateContext performs no pricing, admission, or provider I/O. It uses the
// configured tokenizer, or the same approximate input estimate as reservation.
// The media allowance is left out: reservation caps it at the room this check
// leaves, so it can never change the outcome.
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
	// The reasoning budget is not added. Every supported family counts
	// thinking inside the output cap: Anthropic thinking.budget_tokens "must be
	// ≥1024 and less than max_tokens", and OpenAI max_output_tokens and
	// max_completion_tokens bound "visible output tokens and reasoning tokens".
	// Bedrock Converse forwards no reasoning controls.
	return validateContextCounts(candidate.ContextTokens, input, output)
}

// validateContextCounts checks the window against the input and the output
// cap. Reasoning tokens are generated inside the output cap, so they occupy no
// additional room.
func validateContextCounts(limit, input, output int64) error {
	if limit < 0 || input < 0 || output < 0 {
		return fmt.Errorf("context limit and token counts must not be negative")
	}
	if limit == 0 {
		return nil
	}
	// Subtraction avoids overflow even for adversarial int64 token counts.
	remaining := limit
	for _, count := range []int64{input, output} {
		if count > remaining {
			return ErrContextLimit
		}
		remaining -= count
	}
	return nil
}
