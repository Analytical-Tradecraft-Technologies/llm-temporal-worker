package contractkit_test

import (
	"bytes"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/contractkit"
)

func kitFixture(t *testing.T, kit *contractkit.Kit, name string) []byte {
	t.Helper()
	data, ok := kit.File(contractkit.FixtureDir + "/" + name)
	if !ok {
		t.Fatalf("kit has no fixture %s", name)
	}
	return data
}

// The null-or-[]-means-absent allowance and the instant spelling allowance
// cover only named closed contract members. Open JSON payloads, such as a
// JSON content value or tool arguments, must be carried exactly.
func TestCheckEncodingComparesOpenJSONExactly(t *testing.T) {
	kit := embeddedKit(t)
	request := kitFixture(t, kit, "generate-request-all-kinds.json")
	for name, test := range map[string]struct{ old, new string }{
		"null member in JSON content":   {`{"claim": 4831, "flags"`, `{"claim": 4831, "extra": null, "flags"`},
		"empty array in JSON content":   {`{"claim": 4831, "flags"`, `{"claim": 4831, "extra": [], "flags"`},
		"null member in tool arguments": {`"arguments": {"claim": 4831}`, `"arguments": {"claim": 4831, "extra": null}`},
	} {
		t.Run(name, func(t *testing.T) {
			encoded := bytes.Replace(request, []byte(test.old), []byte(test.new), 1)
			if bytes.Equal(encoded, request) {
				t.Fatalf("fixture does not contain %q", test.old)
			}
			err := kit.CheckEncoding("generate-request-all-kinds.json", encoded)
			if err == nil || !strings.Contains(err.Error(), "unexpected member") {
				t.Fatalf("open JSON with an added member passed: %v", err)
			}
		})
	}

	response := kitFixture(t, kit, "generate-response.json")
	omitted := bytes.Replace(response, []byte(`,"diagnostics":[]`), nil, 1)
	if bytes.Equal(omitted, response) {
		t.Fatal("generate-response.json has no empty diagnostics")
	}
	if err := kit.CheckEncoding("generate-response.json", omitted); err != nil {
		t.Fatalf("omitting empty diagnostics, a named optional member, failed: %v", err)
	}
	spelled := bytes.Replace(kitFixture(t, kit, "query-provider-response.json"), []byte(`926535Z"`), []byte(`926535000-00:00"`), 1)
	if err := kit.CheckEncoding("query-provider-response.json", spelled); err != nil {
		t.Fatalf("respelling observed_at, a named instant, failed: %v", err)
	}
}

// A codec that is a shell script leaves its own children holding the
// output pipes; the timeout must still bound the run.
func TestCommandTimeoutBoundsDescendantsHoldingPipes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("needs a POSIX shell")
	}
	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh on PATH")
	}
	codec := contractkit.Command(50*time.Millisecond, shell, "-c", "sleep 5; true")
	started := time.Now()
	_, err = codec(contractkit.GenerateRequest, "generate-root.json", []byte("{}"))
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("timed-out codec returned after %s", elapsed)
	}
	if err == nil || !strings.Contains(err.Error(), "did not finish within 50ms") {
		t.Fatalf("timed-out codec error = %v", err)
	}
}
