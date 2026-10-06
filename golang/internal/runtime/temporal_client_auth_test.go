package runtime

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"go.temporal.io/sdk/client"
)

func testClientCertificatePEM(t *testing.T) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "llmtw worker"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// The Temporal client presents a configured mTLS client certificate or API
// key, so it can reach a frontend that requires client authentication (#996).
func TestTemporalFactoryPresentsClientCredentials(t *testing.T) {
	ca := testCertificateAuthorityPEM(t)
	cert, key := testClientCertificatePEM(t)
	files := map[string][]byte{"/ca": ca, "/cert": cert, "/key": key, "/api-key": []byte("secret-api-key\n"), "/bad-key": []byte("not a key"), "/empty": []byte(" \n")}
	read := func(path string) ([]byte, error) {
		if value, ok := files[path]; ok {
			return value, nil
		}
		return nil, errors.New("missing")
	}
	for _, tc := range []struct {
		name      string
		tls       config.TLSConfig
		apiKey    string
		wantError string
		check     func(*testing.T, client.Options)
	}{
		{name: "mtls", tls: config.TLSConfig{CertFile: "/cert", KeyFile: "/key"}, check: func(t *testing.T, options client.Options) {
			if len(options.ConnectionOptions.TLS.Certificates) != 1 || options.Credentials != nil {
				t.Fatalf("mTLS options = %+v", options.ConnectionOptions.TLS)
			}
		}},
		{name: "api key", apiKey: "/api-key", check: func(t *testing.T, options client.Options) {
			if options.Credentials == nil || len(options.ConnectionOptions.TLS.Certificates) != 0 {
				t.Fatal("API key credentials not installed")
			}
		}},
		{name: "invalid key pair", tls: config.TLSConfig{CertFile: "/cert", KeyFile: "/bad-key"}, wantError: "Temporal TLS client certificate or key is invalid"},
		{name: "missing certificate", tls: config.TLSConfig{CertFile: "/missing", KeyFile: "/key"}, wantError: "read Temporal TLS client certificate"},
		{name: "empty api key", apiKey: "/empty", wantError: "Temporal API key is invalid"},
		{name: "missing api key", apiKey: "/missing", wantError: "read Temporal API key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dialed *client.Options
			factory := DefaultTemporalClientFactory{ReadFile: read, DialContext: func(_ context.Context, options client.Options) (client.Client, error) {
				dialed = &options
				return nil, nil
			}}
			value := config.Config{}
			value.Temporal.TLS = tc.tls
			value.Temporal.TLS.Enabled, value.Temporal.TLS.CAFile, value.Temporal.TLS.ServerName = true, "/ca", "temporal.example.test"
			value.Temporal.APIKeyFile = tc.apiKey
			_, err := factory.New(context.Background(), value)
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError || dialed != nil {
					t.Fatalf("error = %v, want %q without dialing", err, tc.wantError)
				}
				if strings.Contains(err.Error(), "secret-api-key") {
					t.Fatal("error exposes the API key")
				}
				return
			}
			if err != nil || dialed == nil {
				t.Fatalf("New() = %v", err)
			}
			tc.check(t, *dialed)
		})
	}
}
