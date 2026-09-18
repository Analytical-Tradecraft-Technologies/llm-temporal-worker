package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/buildinfo"
	"github.com/mfow/llm-temporal-worker/golang/internal/httpserver"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
)

const (
	defaultConfigPath    = "/etc/llmtw/config.yaml"
	defaultHealthAddress = "0.0.0.0:8080"
)

var errWorkerRuntimeUnavailable = errors.New("worker runtime dependencies are unavailable")

type CommandOptions struct {
	Out       io.Writer
	ErrOut    io.Writer
	Resolver  config.ReferenceResolver
	RunWorker func(context.Context, []byte, io.Writer) error
	// RunWorkerFile receives the source path as well as its initially
	// validated bytes so the production runtime can watch the same file for
	// SIGHUP and atomic replacement reloads. RunWorker remains a compatibility
	// seam for small embeddings that own their lifecycle trigger.
	RunWorkerFile      func(context.Context, string, []byte, io.Writer) error
	RunBudgetBootstrap func(context.Context, BudgetBootstrapCommandRequest) (redisstore.BudgetColdStartReceipt, error)
	LookupEnv          func(string) (string, bool)
}

func Execute(ctx context.Context, args []string, options CommandOptions) int {
	if ctx == nil {
		ctx = context.Background()
	}
	if options.Out == nil {
		options.Out = io.Discard
	}
	if options.ErrOut == nil {
		options.ErrOut = io.Discard
	}
	if len(args) == 0 {
		writeCommandError(options.ErrOut, errors.New("a command is required"))
		return 2
	}
	switch args[0] {
	case "version":
		return executeVersionCommand(options)
	case "health-server":
		return executeHealthServerCommand(ctx, args[1:], options)
	case "validate-config":
		return executeConfigCommand(ctx, args[1:], options, false)
	case "print-effective-config":
		return executeConfigCommand(ctx, args[1:], options, true)
	case "worker":
		return executeWorkerCommand(ctx, args[1:], options)
	case "healthcheck":
		return executeHealthcheckCommand(ctx, args[1:], options)
	case "budget-bootstrap":
		return executeBudgetBootstrapCommand(ctx, args[1:], options)
	case "help", "-h", "--help":
		writeUsage(options.Out)
		return 0
	default:
		writeCommandError(options.ErrOut, fmt.Errorf("unknown command %q", args[0]))
		return 2
	}
}

func executeVersionCommand(options CommandOptions) int {
	if err := json.NewEncoder(options.Out).Encode(buildinfo.Current()); err != nil {
		writeCommandError(options.ErrOut, errors.New("write build metadata"))
		return 1
	}
	return 0
}

// executeHealthServerCommand serves only the process health surface for
// hardened-image verification. It is live while its listener is responsive and
// intentionally never reports ready because it does not construct the worker
// or validate its dependencies.
func executeHealthServerCommand(ctx context.Context, args []string, options CommandOptions) int {
	flags := flag.NewFlagSet("health-server", flag.ContinueOnError)
	flags.SetOutput(options.ErrOut)
	address := flags.String("address", defaultHealthAddress, "health listener address")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return 2
	}
	state := httpserver.NewHealthState()
	server, err := httpserver.New(httpserver.Options{Address: *address, Health: state})
	if err != nil {
		writeCommandError(options.ErrOut, err)
		return 1
	}
	if err := server.Start(); err != nil {
		writeCommandError(options.ErrOut, err)
		return 1
	}
	if _, err := fmt.Fprintln(options.Out, server.Addr()); err != nil {
		state.SetLive(false)
		_ = server.Shutdown(context.Background())
		writeCommandError(options.ErrOut, errors.New("write health listener address"))
		return 1
	}
	<-ctx.Done()
	state.SetLive(false)
	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		writeCommandError(options.ErrOut, errors.New("shutdown health listener"))
		return 1
	}
	return 0
}

type healthcheckURLs []string

func (urls *healthcheckURLs) String() string { return strings.Join(*urls, ",") }

func (urls *healthcheckURLs) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("healthcheck URL is required")
	}
	*urls = append(*urls, value)
	return nil
}

func executeHealthcheckCommand(ctx context.Context, args []string, options CommandOptions) int {
	flags := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	flags.SetOutput(options.ErrOut)
	var endpoints healthcheckURLs
	flags.Var(&endpoints, "url", "HTTP(S) URL to require healthy; may be repeated")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if len(endpoints) == 0 || flags.NArg() != 0 {
		writeCommandError(options.ErrOut, errors.New("at least one healthcheck URL is required"))
		return 2
	}
	client := &http.Client{
		Timeout: 2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for _, endpoint := range endpoints {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil || request.URL.Scheme == "" || request.URL.Host == "" || (request.URL.Scheme != "http" && request.URL.Scheme != "https") {
			writeCommandError(options.ErrOut, errors.New("healthcheck URL must be an absolute HTTP(S) URL"))
			return 2
		}
		response, err := client.Do(request)
		if err != nil {
			writeCommandError(options.ErrOut, errors.New("healthcheck failed"))
			return 1
		}
		_ = response.Body.Close()
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			writeCommandError(options.ErrOut, errors.New("healthcheck failed"))
			return 1
		}
	}
	_, _ = io.WriteString(options.Out, "healthcheck passed\n")
	return 0
}

func executeConfigCommand(ctx context.Context, args []string, options CommandOptions, printEffective bool) int {
	flags := flag.NewFlagSet("config", flag.ContinueOnError)
	flags.SetOutput(options.ErrOut)
	path := flags.String("config", defaultConfigPath, "configuration YAML path")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	data, err := readConfig(*path)
	if err != nil {
		writeCommandError(options.ErrOut, err)
		return 1
	}
	snapshot, err := config.Compile(ctx, data, options.Resolver)
	if err != nil {
		writeCommandError(options.ErrOut, err)
		return 1
	}
	if printEffective {
		_, _ = options.Out.Write(snapshot.Canonical())
		_, _ = io.WriteString(options.Out, "\n")
		return 0
	}
	_, _ = fmt.Fprintf(options.Out, "valid config version %s\n", snapshot.ConfigVersion())
	return 0
}

func executeWorkerCommand(ctx context.Context, args []string, options CommandOptions) int {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	flags.SetOutput(options.ErrOut)
	path := flags.String("config", defaultConfigPath, "configuration YAML path")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	data, err := readConfig(*path)
	if err != nil {
		writeCommandError(options.ErrOut, err)
		return 1
	}
	if _, err := config.Compile(ctx, data, options.Resolver); err != nil {
		writeCommandError(options.ErrOut, err)
		return 1
	}
	if options.RunWorkerFile == nil && options.RunWorker == nil {
		writeCommandError(options.ErrOut, errWorkerRuntimeUnavailable)
		return 1
	}
	var runErr error
	if options.RunWorkerFile != nil {
		runErr = options.RunWorkerFile(ctx, *path, data, options.Out)
	} else {
		runErr = options.RunWorker(ctx, data, options.Out)
	}
	if runErr != nil {
		writeCommandError(options.ErrOut, runErr)
		return 1
	}
	return 0
}

func readConfig(path string) ([]byte, error) {
	if path == "" {
		return nil, errors.New("config path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read configuration: %w", err)
	}
	defer file.Close()
	const maxBytes = 4 << 20
	data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, errors.New("read configuration failed")
	}
	if len(data) > maxBytes {
		return nil, errors.New("configuration exceeds safe size")
	}
	return data, nil
}

func writeCommandError(output io.Writer, err error) {
	if output == nil {
		return
	}
	message := "command failed"
	if err != nil {
		candidate := strings.TrimSpace(strings.SplitN(err.Error(), "\n", 2)[0])
		lower := strings.ToLower(candidate)
		for _, word := range []string{"secret", "password", "token", "authorization", "credential", "prompt", "output", "provider body"} {
			if strings.Contains(lower, word) {
				candidate = "command failed"
				break
			}
		}
		if len(candidate) > 240 {
			candidate = "command failed"
		}
		if candidate != "" {
			message = candidate
		}
	}
	_, _ = fmt.Fprintf(output, "%s\n", message)
}

func writeUsage(output io.Writer) {
	_, _ = io.WriteString(output, "usage: llm-temporal-worker <version|health-server|worker|budget-bootstrap|validate-config|print-effective-config|healthcheck>\n")
}
