package runtime

import (
	"bytes"
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"go.temporal.io/sdk/client"
)

func TestTemporalFactoryReadsProjectedCA(t *testing.T) {
	ca := testCertificateAuthorityPEM(t)
	for _, tc := range []struct {
		name      string
		contents  []byte
		wantError string
	}{
		{name: "valid", contents: ca},
		{name: "at size limit", contents: append(append([]byte{}, ca...), bytes.Repeat([]byte("\n"), (1<<20)-len(ca))...)},
		{name: "oversized", contents: append(append([]byte{}, ca...), bytes.Repeat([]byte("\n"), (1<<20)+1-len(ca))...), wantError: "read Temporal TLS CA certificate"},
		{name: "invalid certificate", contents: []byte("PRIVATE-CERT-MARKER"), wantError: "Temporal TLS CA certificate is invalid"},
		{name: "missing target", wantError: "read Temporal TLS CA certificate"},
		{name: "directory target", wantError: "read Temporal TLS CA certificate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			volume := t.TempDir()
			data := filepath.Join(volume, "..2026_09_20_00_00_00")
			if err := os.Mkdir(data, 0700); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(data, "temporal-ca.pem")
			if tc.contents != nil {
				if err := os.WriteFile(target, tc.contents, 0400); err != nil {
					t.Fatal(err)
				}
			} else if tc.name == "directory target" {
				if err := os.Mkdir(target, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(filepath.Base(data), filepath.Join(volume, "..data")); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(volume, "temporal-ca.pem")
			if err := os.Symlink(filepath.Join("..data", "temporal-ca.pem"), path); err != nil {
				t.Fatal(err)
			}
			called := false
			factory := DefaultTemporalClientFactory{DialContext: func(_ context.Context, options client.Options) (client.Client, error) {
				called = true
				cfg := options.ConnectionOptions.TLS
				if cfg == nil || cfg.RootCAs == nil || len(cfg.RootCAs.Subjects()) != 1 || cfg.ServerName != "temporal.example.test" || cfg.MinVersion != tls.VersionTLS12 {
					t.Fatalf("unexpected TLS configuration: %+v", cfg)
				}
				return nil, nil
			}}
			value := config.Config{}
			value.Temporal.TLS = config.TLSConfig{Enabled: true, CAFile: path, ServerName: "temporal.example.test"}
			_, err := factory.New(context.Background(), value)
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError {
					t.Fatalf("error = %v, want %q", err, tc.wantError)
				}
				if called {
					t.Fatal("dial called with invalid CA")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if !called {
					t.Fatal("dial not called")
				}
			}
		})
	}
}
