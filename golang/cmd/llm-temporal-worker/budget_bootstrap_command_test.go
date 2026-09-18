package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
)

func budgetBootstrapArgs() []string {
	return []string{
		"budget-bootstrap",
		"--redis-address", "redis.example.internal:6379",
		"--redis-server-name", "redis.example.internal",
		"--redis-ca-file", "/var/run/redis/ca.crt",
		"--key-prefix", "llmtw",
		"--admission-hash-tag", "admission",
		"--manifest-file", "/etc/llmtw/budget-manifest.json",
		"--timeout", "10s",
	}
}

func budgetBootstrapEnv(secret string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		switch name {
		case budgetBootstrapUsernameEnv:
			return "worker-bootstrap", true
		case budgetBootstrapPasswordEnv:
			return secret, true
		default:
			return "", false
		}
	}
}

func validBudgetBootstrapReceipt() redisstore.BudgetColdStartReceipt {
	return redisstore.BudgetColdStartReceipt{Schema: redisstore.BudgetColdStartReceiptSchema, ReceiptID: strings.Repeat("a", 64)}
}

func TestBudgetBootstrapCommandUsesDeterministicFlagsAndSecretEnvironment(t *testing.T) {
	secret := "redis-password-marker"
	var got BudgetBootstrapCommandRequest
	var output, errorsOut bytes.Buffer
	code := Execute(context.Background(), budgetBootstrapArgs(), CommandOptions{
		Out: &output, ErrOut: &errorsOut, LookupEnv: budgetBootstrapEnv(secret),
		RunBudgetBootstrap: func(_ context.Context, request BudgetBootstrapCommandRequest) (redisstore.BudgetColdStartReceipt, error) {
			got = request
			return validBudgetBootstrapReceipt(), nil
		},
	})
	if code != 0 || errorsOut.Len() != 0 {
		t.Fatalf("code=%d stderr=%q", code, errorsOut.String())
	}
	if got.RedisAddress != "redis.example.internal:6379" || got.RedisServerName != "redis.example.internal" || got.RedisCAFile != "/var/run/redis/ca.crt" || got.KeyPrefix != "llmtw" || got.AdmissionHashTag != "admission" || got.ManifestFile != "/etc/llmtw/budget-manifest.json" || got.Timeout != 10*time.Second {
		t.Fatalf("request=%#v", got)
	}
	if got.username != "worker-bootstrap" || got.password != secret {
		t.Fatal("fixed secret environment was not handed to bootstrap runner")
	}
	if strings.Contains(output.String(), secret) || output.String() != `{"schema":"budget-cold-start-receipt/v1","receipt_id":"`+strings.Repeat("a", 64)+`"}`+"\n" {
		t.Fatalf("receipt=%q", output.String())
	}
}

func TestBudgetBootstrapCommandRequiresBothACLSecretKeys(t *testing.T) {
	for _, missing := range []string{budgetBootstrapUsernameEnv, budgetBootstrapPasswordEnv} {
		t.Run(missing, func(t *testing.T) {
			called := false
			lookup := func(name string) (string, bool) {
				if name == missing {
					return "", false
				}
				return "present", true
			}
			var errorsOut bytes.Buffer
			code := Execute(context.Background(), budgetBootstrapArgs(), CommandOptions{
				ErrOut: &errorsOut, LookupEnv: lookup,
				RunBudgetBootstrap: func(context.Context, BudgetBootstrapCommandRequest) (redisstore.BudgetColdStartReceipt, error) {
					called = true
					return redisstore.BudgetColdStartReceipt{}, nil
				},
			})
			if code != 2 || called || errorsOut.String() != "budget bootstrap configuration invalid\n" {
				t.Fatalf("code=%d called=%v stderr=%q", code, called, errorsOut.String())
			}
		})
	}
}

func TestBudgetBootstrapCommandRejectsNoncanonicalRedisAddressBeforeRunner(t *testing.T) {
	for _, address := range []string{":6379", " redis.example.internal:6379", "redis.example.internal:0", "redis.example.internal:65536", "redis.example.internal:06379"} {
		t.Run(address, func(t *testing.T) {
			args := budgetBootstrapArgs()
			args[2] = address
			called := false
			var errorsOut bytes.Buffer
			code := Execute(context.Background(), args, CommandOptions{
				ErrOut: &errorsOut, LookupEnv: budgetBootstrapEnv("password"),
				RunBudgetBootstrap: func(context.Context, BudgetBootstrapCommandRequest) (redisstore.BudgetColdStartReceipt, error) {
					called = true
					return redisstore.BudgetColdStartReceipt{}, nil
				},
			})
			if code != 2 || called || errorsOut.String() != "budget bootstrap configuration invalid\n" {
				t.Fatalf("code=%d called=%v stderr=%q", code, called, errorsOut.String())
			}
		})
	}
}

func TestBudgetBootstrapCommandHasStableFailClosedExitCodes(t *testing.T) {
	secret := "do-not-print-this-secret"
	cases := []struct {
		name string
		err  error
		code int
		line string
	}{
		{name: "manifest", err: redisstore.ErrBudgetManifestInvalid, code: 2, line: "budget bootstrap configuration invalid\n"},
		{name: "timeout", err: context.DeadlineExceeded, code: 3, line: "budget bootstrap Redis unavailable\n"},
		{name: "TLS", err: errors.New("TLS failure " + secret), code: 3, line: "budget bootstrap Redis unavailable\n"},
		{name: "auth", err: errors.New("WRONGPASS " + secret), code: 3, line: "budget bootstrap Redis unavailable\n"},
		{name: "function digest", err: redisstore.ErrBudgetColdStartFunctionMismatch, code: 4, line: "budget bootstrap Redis function mismatch\n"},
		{name: "state", err: redisstore.ErrBudgetColdStartStateConflict, code: 5, line: "budget bootstrap state conflict\n"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var output, errorsOut bytes.Buffer
			code := Execute(context.Background(), budgetBootstrapArgs(), CommandOptions{
				Out: &output, ErrOut: &errorsOut, LookupEnv: budgetBootstrapEnv(secret),
				RunBudgetBootstrap: func(context.Context, BudgetBootstrapCommandRequest) (redisstore.BudgetColdStartReceipt, error) {
					return redisstore.BudgetColdStartReceipt{}, test.err
				},
			})
			if code != test.code || output.Len() != 0 || errorsOut.String() != test.line {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, output.String(), errorsOut.String())
			}
			if strings.Contains(errorsOut.String(), secret) {
				t.Fatalf("failure leaked secret: %q", errorsOut.String())
			}
		})
	}
}

func TestBudgetBootstrapCommandRejectsMutableOrMalformedReceipt(t *testing.T) {
	for _, receipt := range []redisstore.BudgetColdStartReceipt{
		{Schema: "other", ReceiptID: strings.Repeat("a", 64)},
		{Schema: redisstore.BudgetColdStartReceiptSchema, ReceiptID: "mutable-state"},
	} {
		var output, errorsOut bytes.Buffer
		code := Execute(context.Background(), budgetBootstrapArgs(), CommandOptions{
			Out: &output, ErrOut: &errorsOut, LookupEnv: budgetBootstrapEnv("secret"),
			RunBudgetBootstrap: func(context.Context, BudgetBootstrapCommandRequest) (redisstore.BudgetColdStartReceipt, error) {
				return receipt, nil
			},
		})
		if code != 6 || output.Len() != 0 || errorsOut.String() != "budget bootstrap receipt unavailable\n" {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, output.String(), errorsOut.String())
		}
	}
}

func TestBudgetBootstrapCommandRedactsMalformedArguments(t *testing.T) {
	secret := strings.Repeat("credential-marker", 128)
	for _, args := range [][]string{
		append(budgetBootstrapArgs(), "--timeout", secret),
		append(budgetBootstrapArgs(), secret),
	} {
		var output, errorsOut bytes.Buffer
		code := Execute(context.Background(), args, CommandOptions{
			Out: &output, ErrOut: &errorsOut, LookupEnv: budgetBootstrapEnv("password"),
		})
		if code != 2 || output.Len() != 0 || errorsOut.String() != "budget bootstrap configuration invalid\n" {
			t.Fatalf("code=%d stdout=%q stderr=%q", code, output.String(), errorsOut.String())
		}
		if strings.Contains(errorsOut.String(), secret) || errorsOut.Len() > 64 {
			t.Fatalf("malformed argument was not bounded and redacted: %q", errorsOut.String())
		}
	}
}

func TestLoadBudgetBootstrapTLSRequiresVerifiedCAAndServerName(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	certificate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "bootstrap-test-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, privateKey.Public(), privateKey)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(t.TempDir(), "redis-ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := loadBudgetBootstrapTLS(caFile, "redis.example.internal")
	if err != nil {
		t.Fatal(err)
	}
	if config.MinVersion != tls.VersionTLS12 || config.ServerName != "redis.example.internal" || config.RootCAs == nil {
		t.Fatalf("TLS config=%#v", config)
	}
	if _, err := loadBudgetBootstrapTLS(caFile, ""); !errors.Is(err, errBudgetBootstrapConfiguration) {
		t.Fatalf("missing server name error=%v", err)
	}
	badCA := filepath.Join(t.TempDir(), "bad-ca.pem")
	if err := os.WriteFile(badCA, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadBudgetBootstrapTLS(badCA, "redis.example.internal"); !errors.Is(err, errBudgetBootstrapConfiguration) {
		t.Fatalf("invalid CA error=%v", err)
	}
}
