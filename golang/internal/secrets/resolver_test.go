package secrets

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
)

func TestDefaultResolverSourcesAndCopies(t *testing.T) {
	temporary := t.TempDir()
	path := filepath.Join(temporary, "token")
	if err := os.WriteFile(path, []byte("file-secret"), 0600); err != nil {
		t.Fatal(err)
	}
	resolver := New(Options{
		LookupEnv: func(name string) (string, bool) {
			if name == "TOKEN" {
				return "env-secret", true
			}
			return "", false
		},
		Workload: WorkloadIdentityFunc(func(_ context.Context, audience string) ([]byte, error) {
			return []byte("workload-" + audience), nil
		}),
	})
	checks := []struct {
		name string
		ref  config.SecretRef
		want string
	}{
		{name: "env", ref: config.SecretRef{Kind: config.SecretEnv, Name: "TOKEN"}, want: "env-secret"},
		{name: "file", ref: config.SecretRef{Kind: config.SecretFile, Path: path}, want: "file-secret"},
		{name: "workload", ref: config.SecretRef{Kind: config.SecretWorkloadIdentity, Audience: "aud"}, want: "workload-aud"},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			got, err := resolver.Resolve(context.Background(), check.ref)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != check.want {
				t.Fatalf("secret = %q, want %q", got, check.want)
			}
			got[0] = 'X'
			again, err := resolver.Resolve(context.Background(), check.ref)
			if err != nil {
				t.Fatal(err)
			}
			if string(again) != check.want {
				t.Fatalf("resolver returned mutable shared value %q", again)
			}
		})
	}
}

func TestDefaultResolverRejectsMissingOversizedAndCanceled(t *testing.T) {
	resolver := New(Options{MaxBytes: 3, LookupEnv: func(string) (string, bool) { return "", false }})
	if _, err := resolver.Resolve(context.Background(), config.SecretRef{Kind: config.SecretEnv, Name: "MISSING"}); err == nil || strings.Contains(err.Error(), "secret-value") {
		t.Fatalf("missing secret error = %v", err)
	}
	temporary := filepath.Join(t.TempDir(), "large")
	if err := os.WriteFile(temporary, []byte("abcd"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(context.Background(), config.SecretRef{Kind: config.SecretFile, Path: temporary}); err == nil {
		t.Fatal("oversized file unexpectedly resolved")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(canceled, config.SecretRef{Kind: config.SecretEnv, Name: "TOKEN"}); err != context.Canceled {
		t.Fatalf("canceled error = %v", err)
	}
}

func TestConfigResolverDiscardsResolvedValues(t *testing.T) {
	var seen []config.SecretRef
	resolver := ConfigResolver{Resolver: ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
		seen = append(seen, ref)
		return []byte("do-not-store"), nil
	})}
	value := &config.Config{State: config.StateConfig{Redis: config.RedisConfig{
		Username: config.SecretRef{Kind: config.SecretEnv, Name: "USER"},
		Password: config.SecretRef{Kind: config.SecretEnv, Name: "PASS"},
	}}, Continuation: config.ContinuationConfig{HandleKeys: []config.HandleKey{{Secret: config.SecretRef{Kind: config.SecretEnv, Name: "HANDLE"}}}}}
	if err := resolver.Resolve(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 {
		t.Fatalf("resolved %d references, want 3", len(seen))
	}
}

func TestConfigResolverServiceMeshSkipsRedisCredentials(t *testing.T) {
	var seen []config.SecretRef
	resolver := ConfigResolver{Resolver: ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
		seen = append(seen, ref)
		return []byte("fixture"), nil
	})}
	value := &config.Config{State: config.StateConfig{Kind: config.StateKindDurable, Redis: config.RedisConfig{ServiceMesh: true}}, Continuation: config.ContinuationConfig{HandleKeys: []config.HandleKey{{Secret: config.SecretRef{Kind: config.SecretEnv, Name: "HANDLE"}}}}}
	if err := resolver.Resolve(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0].Name != "HANDLE" {
		t.Fatalf("resolved references = %v", seen)
	}
}
