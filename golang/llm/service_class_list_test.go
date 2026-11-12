package llm_test

import (
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestServiceClassesListsPublicClassesInOrder(t *testing.T) {
	want := []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard, llm.ServiceClassPriority}
	got := llm.ServiceClasses()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ServiceClasses = %v, want %v", got, want)
	}
	for _, class := range got {
		if !class.Valid() {
			t.Fatalf("ServiceClasses returned invalid class %q", class)
		}
	}
	got[0] = "changed"
	if llm.ServiceClasses()[0] != llm.ServiceClassEconomy {
		t.Fatal("ServiceClasses returned a shared slice")
	}
}
