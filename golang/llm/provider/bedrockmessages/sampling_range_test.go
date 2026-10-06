package bedrockmessages

import (
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestLowerSamplingRejectsTemperatureOutsideTheProviderRange(t *testing.T) {
	for _, value := range []float64{-0.1, 1.5} {
		temperature := value
		if err := lowerSampling(llm.SamplingSpec{Temperature: &temperature}, map[string]any{}); err == nil {
			t.Fatalf("temperature %v was accepted", value)
		}
	}
	for _, value := range []float64{0, 0.7, 1} {
		temperature := value
		target := map[string]any{}
		if err := lowerSampling(llm.SamplingSpec{Temperature: &temperature}, target); err != nil || target["temperature"] != value {
			t.Fatalf("temperature %v = %v, %v; want it sent", value, target["temperature"], err)
		}
	}
}
