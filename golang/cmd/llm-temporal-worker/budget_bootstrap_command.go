package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
	redisclient "github.com/redis/go-redis/v9"
)

const (
	budgetBootstrapUsernameEnv = "LLMTW_BOOTSTRAP_REDIS_USERNAME"
	budgetBootstrapPasswordEnv = "LLMTW_BOOTSTRAP_REDIS_PASSWORD"
	maxBudgetBootstrapTimeout  = time.Minute
)

var (
	errBudgetBootstrapConfiguration = errors.New("budget bootstrap configuration invalid")
	errBudgetBootstrapUnavailable   = errors.New("budget bootstrap Redis unavailable")
)

// BudgetBootstrapCommandRequest is the complete noninteractive handoff from
// deterministic flags and fixed-name secret environment variables. Credentials
// are intentionally private fields so command fakes cannot accidentally encode
// or print them as part of a receipt.
type BudgetBootstrapCommandRequest struct {
	RedisAddress     string
	RedisServerName  string
	RedisCAFile      string
	KeyPrefix        string
	AdmissionHashTag string
	ManifestFile     string
	Timeout          time.Duration
	username         string
	password         string
}

func executeBudgetBootstrapCommand(ctx context.Context, args []string, options CommandOptions) int {
	flags := flag.NewFlagSet("budget-bootstrap", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	request := BudgetBootstrapCommandRequest{}
	flags.StringVar(&request.RedisAddress, "redis-address", "", "dedicated Redis host:port")
	flags.StringVar(&request.RedisServerName, "redis-server-name", "", "Redis TLS certificate server name")
	flags.StringVar(&request.RedisCAFile, "redis-ca-file", "", "Redis TLS CA PEM file")
	flags.StringVar(&request.KeyPrefix, "key-prefix", "", "immutable Redis key prefix")
	flags.StringVar(&request.AdmissionHashTag, "admission-hash-tag", "admission", "immutable Redis Cluster hash tag")
	flags.StringVar(&request.ManifestFile, "manifest-file", "", "strict initial budget manifest JSON file")
	flags.DurationVar(&request.Timeout, "timeout", 10*time.Second, "bounded bootstrap deadline")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		writeBudgetBootstrapError(options.ErrOut, 2)
		return 2
	}
	lookup := options.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	request.username, _ = lookup(budgetBootstrapUsernameEnv)
	request.password, _ = lookup(budgetBootstrapPasswordEnv)
	if err := validateBudgetBootstrapCommandRequest(request); err != nil {
		writeBudgetBootstrapError(options.ErrOut, 2)
		return 2
	}
	run := options.RunBudgetBootstrap
	if run == nil {
		run = runBudgetBootstrap
	}
	receipt, err := run(ctx, request)
	if err != nil {
		code := budgetBootstrapExitCode(err)
		writeBudgetBootstrapError(options.ErrOut, code)
		return code
	}
	if err := receipt.Validate(); err != nil {
		writeBudgetBootstrapError(options.ErrOut, 6)
		return 6
	}
	if err := json.NewEncoder(options.Out).Encode(receipt); err != nil {
		writeBudgetBootstrapError(options.ErrOut, 6)
		return 6
	}
	return 0
}

func validateBudgetBootstrapCommandRequest(request BudgetBootstrapCommandRequest) error {
	for _, value := range []string{request.RedisAddress, request.RedisServerName, request.RedisCAFile, request.KeyPrefix, request.AdmissionHashTag, request.ManifestFile, request.username, request.password} {
		if strings.TrimSpace(value) == "" {
			return errBudgetBootstrapConfiguration
		}
	}
	if request.Timeout <= 0 || request.Timeout > maxBudgetBootstrapTimeout {
		return errBudgetBootstrapConfiguration
	}
	host, rawPort, err := net.SplitHostPort(request.RedisAddress)
	if err != nil || strings.TrimSpace(host) == "" || host != strings.TrimSpace(host) {
		return errBudgetBootstrapConfiguration
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port < 1 || port > 65535 || rawPort != strconv.Itoa(port) {
		return errBudgetBootstrapConfiguration
	}
	return nil
}

func runBudgetBootstrap(parent context.Context, request BudgetBootstrapCommandRequest) (redisstore.BudgetColdStartReceipt, error) {
	if parent == nil {
		parent = context.Background()
	}
	if err := validateBudgetBootstrapCommandRequest(request); err != nil {
		return redisstore.BudgetColdStartReceipt{}, err
	}
	manifest, err := readBudgetBootstrapManifest(request.ManifestFile)
	if err != nil {
		return redisstore.BudgetColdStartReceipt{}, err
	}
	if err := redisstore.ValidateBudgetColdStartManifest(manifest); err != nil {
		return redisstore.BudgetColdStartReceipt{}, err
	}
	tlsConfig, err := loadBudgetBootstrapTLS(request.RedisCAFile, request.RedisServerName)
	if err != nil {
		return redisstore.BudgetColdStartReceipt{}, err
	}
	ctx, cancel := context.WithTimeout(parent, request.Timeout)
	defer cancel()
	client := redisclient.NewClient(&redisclient.Options{
		Addr: request.RedisAddress, Username: request.username, Password: request.password,
		DialTimeout: request.Timeout, ReadTimeout: request.Timeout, WriteTimeout: request.Timeout,
		MaxRetries: -1, TLSConfig: tlsConfig,
	})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		return redisstore.BudgetColdStartReceipt{}, fmt.Errorf("%w: connection", errBudgetBootstrapUnavailable)
	}
	keySecret := sha256.Sum256(append([]byte("llmtw:redis-key-v1:"), []byte(request.password)...))
	keyOptions, err := redisstore.NewKeyOptions(request.KeyPrefix, request.AdmissionHashTag, keySecret[:])
	if err != nil {
		return redisstore.BudgetColdStartReceipt{}, errBudgetBootstrapConfiguration
	}
	keys, err := redisstore.NewBudgetKeySpace(keyOptions)
	if err != nil {
		return redisstore.BudgetColdStartReceipt{}, errBudgetBootstrapConfiguration
	}
	receipt, err := redisstore.BootstrapBudgetColdStart(ctx, redisstore.BudgetColdStartOptions{Client: client, Keys: keys, Manifest: manifest})
	if err != nil {
		return redisstore.BudgetColdStartReceipt{}, err
	}
	return receipt, nil
}

func readBudgetBootstrapManifest(path string) (redisstore.BudgetManifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return redisstore.BudgetManifest{}, errBudgetBootstrapConfiguration
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > redisstore.MaxBudgetManifestBytes {
		return redisstore.BudgetManifest{}, errBudgetBootstrapConfiguration
	}
	data, err := io.ReadAll(io.LimitReader(file, redisstore.MaxBudgetManifestBytes+1))
	if err != nil || len(data) == 0 || len(data) > redisstore.MaxBudgetManifestBytes {
		return redisstore.BudgetManifest{}, errBudgetBootstrapConfiguration
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest redisstore.BudgetManifest
	if err := decoder.Decode(&manifest); err != nil {
		return redisstore.BudgetManifest{}, errBudgetBootstrapConfiguration
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return redisstore.BudgetManifest{}, errBudgetBootstrapConfiguration
	}
	if _, err := manifest.Canonical(); err != nil {
		return redisstore.BudgetManifest{}, errBudgetBootstrapConfiguration
	}
	return manifest, nil
}

func loadBudgetBootstrapTLS(caFile, serverName string) (*tls.Config, error) {
	if strings.TrimSpace(caFile) == "" || strings.TrimSpace(serverName) == "" {
		return nil, errBudgetBootstrapConfiguration
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, errBudgetBootstrapConfiguration
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errBudgetBootstrapConfiguration
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName, RootCAs: roots}, nil
}

func budgetBootstrapExitCode(err error) int {
	switch {
	case errors.Is(err, errBudgetBootstrapConfiguration), errors.Is(err, redisstore.ErrBudgetManifestInvalid):
		return 2
	case errors.Is(err, redisstore.ErrBudgetColdStartFunctionMismatch):
		return 4
	case errors.Is(err, redisstore.ErrBudgetColdStartStateConflict):
		return 5
	default:
		return 3
	}
}

func writeBudgetBootstrapError(output io.Writer, code int) {
	message := map[int]string{
		2: "budget bootstrap configuration invalid",
		3: "budget bootstrap Redis unavailable",
		4: "budget bootstrap Redis function mismatch",
		5: "budget bootstrap state conflict",
		6: "budget bootstrap receipt unavailable",
	}[code]
	if message == "" {
		message = "budget bootstrap failed"
	}
	_, _ = fmt.Fprintln(output, message)
}
