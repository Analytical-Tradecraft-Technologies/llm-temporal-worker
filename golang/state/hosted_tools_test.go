package state

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"testing"
)

func TestHostedToolsPersistAndExplicitlyDisable(t *testing.T) {
	yes, no := true, false
	root, err := ApplySettingsPatchV1(RootModelState("model"), llm.SettingsPatchV1{WebSearch: llm.Patch[bool]{Set: &yes}, WebFetch: llm.Patch[bool]{Set: &yes}, CodeExecution: llm.Patch[bool]{Set: &yes}})
	if err != nil {
		t.Fatal(err)
	}
	wire := settingsPatchToWire(settingsPatchFromModel(root))
	restored, err := ApplySettingsPatchV1(RootModelState("other"), wire)
	if err != nil {
		t.Fatal(err)
	}
	if !restored.WebSearch || !restored.WebFetch || !restored.CodeExecution {
		t.Fatal("checkpoint flags lost")
	}
	disabled, err := ApplySettingsPatchV1(restored, llm.SettingsPatchV1{WebSearch: llm.Patch[bool]{Set: &no}, WebFetch: llm.Patch[bool]{Clear: true}, CodeExecution: llm.Patch[bool]{Set: &no}})
	if err != nil {
		t.Fatal(err)
	}
	if disabled.WebSearch || disabled.WebFetch || disabled.CodeExecution {
		t.Fatal("false/clear did not disable tools")
	}
	if !root.WebSearch || !root.WebFetch || !root.CodeExecution {
		t.Fatal("child patch mutated root")
	}
}
