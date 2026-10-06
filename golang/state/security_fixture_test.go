package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestContinuationSecurityHandleFixturesFailClosed(t *testing.T) {
	keyring, err := NewKeyring([]Key{{ID: "k1", Secret: bytes.Repeat([]byte{1}, 32), Primary: true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range securityFixtureLines(t, "handles.txt") {
		t.Run(line, func(t *testing.T) {
			if _, err := keyring.Verify("tenant-a", line); !errors.Is(err, ErrInvalidHandle) {
				t.Fatalf("malformed handle was accepted: %v", err)
			}
		})
	}
	valid, err := keyring.Issue("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := keyring.Verify("tenant-b", valid); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("cross-tenant handle error = %v, want ErrInvalidHandle", err)
	}
}

func TestContinuationSecurityRecordFixturesFailClosed(t *testing.T) {
	data := readSecurityFixture(t, "records.json")
	var fixtures []struct {
		Name           string `json:"name"`
		Provider       string `json:"provider"`
		Endpoint       string `json:"endpoint"`
		Family         string `json:"family"`
		Media          string `json:"media"`
		Data           string `json:"data"`
		Depth          int    `json:"depth"`
		DigestMismatch bool   `json:"digest_mismatch"`
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			transcript := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "safe"}}}}
			_, digest, err := CanonicalTranscript(transcript)
			if err != nil {
				t.Fatal(err)
			}
			if fixture.DigestMismatch {
				digest = sha256.Sum256([]byte("swapped-blob"))
			}
			continuation := Continuation{
				ID: "ctn_v1.k1.fixture", Tenant: "tenant-a", Transcript: transcript,
				TranscriptDigest: digest, ExpiresAt: now.Add(time.Hour), Depth: fixture.Depth,
				ProviderState: []OpaqueStateRef{{Provider: fixture.Provider, EndpointID: fixture.Endpoint, Family: fixture.Family, Media: fixture.Media, Data: []byte(fixture.Data)}},
			}
			if err := continuation.Validate(now); err == nil {
				t.Fatal("malformed continuation record was accepted")
			}
		})
	}
}

func securityFixtureLines(t *testing.T, name string) []string {
	t.Helper()
	lines := strings.Split(string(readSecurityFixture(t, name)), "\n")
	result := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			result = append(result, line)
		}
	}
	return result
}

func readSecurityFixture(t *testing.T, name string) []byte {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller unavailable")
	}
	moduleRoot := filepath.Dir(filepath.Dir(source))
	data, err := os.ReadFile(filepath.Join(moduleRoot, "testdata", "contracts", "security", "continuation", name))
	if err != nil {
		t.Fatalf("read security fixture %s: %v", name, err)
	}
	return data
}
