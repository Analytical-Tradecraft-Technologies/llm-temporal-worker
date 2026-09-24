//go:build linux

package codexcli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func privateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return fmt.Errorf("private directory required")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("private directory owner mismatch")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return fmt.Errorf("private directory must not traverse symlinks")
	}
	return nil
}

func pinnedExecutable(config Config) (*os.File, error) {
	fd, err := syscall.Open(config.Executable, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("codex_cli pinned executable unavailable")
	}
	file := os.NewFile(uintptr(fd), config.Executable)
	fail := func() (*os.File, error) {
		_ = file.Close()
		return nil, fmt.Errorf("codex_cli executable must be the pinned native ELF, owner-controlled and executable")
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || info.Mode().Perm()&0111 == 0 {
		return fail()
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || (stat.Uid != uint32(os.Geteuid()) && stat.Uid != 0) {
		return fail()
	}
	var magic [4]byte
	if _, err := io.ReadFull(file, magic[:]); err != nil || magic != [4]byte{0x7f, 'E', 'L', 'F'} {
		return fail()
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fail()
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil || hex.EncodeToString(hash.Sum(nil)) != config.ExecutableSHA256 {
		return fail()
	}
	return file, nil
}

func lockAuthHome(ctx context.Context, home string) (*os.File, error) {
	if err := privateDirectory(home); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(filepath.Join(home, ".llmtw-codex.lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, fmt.Errorf("codex_cli auth serialization unavailable")
	}
	file := os.NewFile(uintptr(fd), "codex-cli-auth-lock")
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		_ = file.Close()
		return nil, fmt.Errorf("codex_cli auth lock is not private")
	}
	for {
		if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			return file, nil
		} else if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = file.Close()
			return nil, fmt.Errorf("codex_cli auth serialization failed")
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			_ = file.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func childEnvironment(home, cwd string) []string {
	// Do not inherit API keys, OAuth overrides, proxy endpoints, tool paths,
	// debug hooks or dynamic-loader variables from the worker environment.
	// In pinned 0.146.1, CODEX_EXEC_SERVER_URL=none disables *both* local
	// and remote execution environments (exec-server/environment_provider.rs).
	// This removes view_image and apply_patch, not merely shell visibility.
	return []string{"HOME=" + cwd, "CODEX_HOME=" + home, "TMPDIR=" + cwd,
		"XDG_CONFIG_HOME=" + cwd, "XDG_CACHE_HOME=" + cwd, "XDG_DATA_HOME=" + cwd,
		"CODEX_EXEC_SERVER_URL=none", "PATH=" + cwd, "LANG=C.UTF-8", "RUST_LOG=off"}
}

func childArguments(model, cwd, schemaPath string) []string {
	args := []string{"exec", "--ignore-user-config", "--ignore-rules", "--strict-config", "--ephemeral", "--json", "--color", "never", "--sandbox", "read-only", "--skip-git-repo-check", "--model", model, "--cd", cwd, "--output-schema", schemaPath}
	settings := []string{
		`forced_login_method="chatgpt"`, `cli_auth_credentials_store="file"`, `approval_policy="never"`,
		`model_provider="llmtw_subscription"`, `model_providers.llmtw_subscription.name="OpenAI"`,
		`model_providers.llmtw_subscription.base_url="https://chatgpt.com/backend-api/codex"`,
		`model_providers.llmtw_subscription.requires_openai_auth=true`,
		`model_providers.llmtw_subscription.request_max_retries=0`, `model_providers.llmtw_subscription.stream_max_retries=0`,
		`model_providers.llmtw_subscription.supports_websockets=false`,
		`web_search="disabled"`, `project_doc_max_bytes=0`, `project_doc_fallback_filenames=[]`,
		`skills.include_instructions=false`, `include_apps_instructions=false`,
		`agents.enabled=false`, `apps._default.enabled=false`, `mcp_servers={}`, `plugins={}`,
		`tools.update_plan.enabled=false`, `tools.experimental_request_user_input.enabled=false`,
		`history.persistence="none"`, `analytics.enabled=false`, `check_for_update_on_startup=false`,
		`model_auto_compact_token_limit=9223372036854775807`, `suppress_unstable_features_warning=true`,
		`log_dir=` + strconv.Quote(cwd), `sqlite_home=` + strconv.Quote(cwd),
	}
	for _, name := range []string{
		"shell_tool", "shell_snapshot", "unified_exec", "code_mode", "code_mode_host",
		"browser_use", "browser_use_external", "computer_use", "apps", "plugins", "hooks",
		"image_generation", "multi_agent", "multi_agent_v2", "goals", "memories",
		"skill_search", "skill_mcp_dependency_install", "tool_suggest", "request_permissions_tool",
		"default_mode_request_user_input", "deferred_executor", "token_budget", "current_time_reminder",
		"standalone_web_search", "remote_plugin", "external_agent_memory_import", "workspace_dependencies",
	} {
		settings = append(settings, "features."+name+"=false")
	}
	for _, setting := range settings {
		args = append(args, "-c", setting)
	}
	return append(args, "-")
}

func isolatedCommand(ctx context.Context, executable *os.File, cwd string, environment, args []string) *exec.Cmd {
	// Execute the already-hashed open inode, not a mutable path or npm wrapper.
	command := exec.CommandContext(ctx, "/proc/self/fd/3", args...)
	command.ExtraFiles = []*os.File{executable}
	command.Dir = cwd
	command.Env = environment
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	command.Cancel = func() error {
		if command.Process == nil {
			return nil
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = time.Second
	return command
}

func (adapter *Adapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	compiled, ok := call.SDKParams.(*compiledCall)
	if !ok || compiled == nil || call.EndpointID != adapter.endpointID || call.Family != provider.FamilyCodexCLI || call.Model != adapter.config.Model || call.OperationKey != compiled.request.OperationKey || !adapter.authorized(compiled.request) {
		return provider.Result{}, rejected(provider.CodePermissionDenied, "codex_cli invocation is not approved")
	}
	if observer == nil {
		return provider.Result{}, rejected(provider.CodeInvalidArgument, "codex_cli dispatch observer is required")
	}
	ctx, cancel := context.WithTimeout(ctx, adapter.timeout)
	defer cancel()
	lock, err := lockAuthHome(ctx, adapter.config.AuthHome)
	if err != nil {
		if ctx.Err() != nil {
			return provider.Result{}, provider.NewPreDispatchContextError(ctx.Err())
		}
		return provider.Result{}, rejected(provider.CodeConfiguration, "codex_cli private auth serialization unavailable")
	}
	defer lock.Close()
	if err := privateDirectory(adapter.config.TempRoot); err != nil {
		return provider.Result{}, rejected(provider.CodeConfiguration, "codex_cli private workspace unavailable")
	}
	// System config can add MCP servers/hooks despite ignoring user config.
	// Only the explicit adapter configuration is admitted in this private mode.
	for _, path := range []string{"/etc/codex/config.toml", "/etc/codex/managed_config.toml"} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			return provider.Result{}, rejected(provider.CodeConfiguration, "codex_cli requires an unmanaged system configuration")
		}
	}
	file, err := pinnedExecutable(adapter.config)
	if err != nil {
		return provider.Result{}, rejected(provider.CodeConfiguration, "codex_cli executable pin validation failed")
	}
	defer file.Close()
	cwd, err := os.MkdirTemp(adapter.config.TempRoot, "llmtw-codex-")
	if err != nil {
		return provider.Result{}, rejected(provider.CodeConfiguration, "codex_cli private workspace unavailable")
	}
	defer os.RemoveAll(cwd)
	environment := childEnvironment(adapter.config.AuthHome, cwd)
	// Version inspection consumes no model quota and does not inspect auth.
	probe := isolatedCommand(ctx, file, cwd, environment, []string{"--version"})
	versionOutput := &boundedStderr{max: 256, cancel: cancel}
	probe.Stdout = versionOutput
	probe.Stderr = &boundedStderr{max: 256, cancel: cancel}
	if err := probe.Run(); err != nil || strings.TrimSpace(string(versionOutput.data)) != "codex-cli "+Version {
		return provider.Result{}, rejected(provider.CodeConfiguration, "codex_cli pinned version is unsupported")
	}
	schemaPath := filepath.Join(cwd, "output.schema.json")
	if err := os.WriteFile(schemaPath, compiled.outputSchema, 0600); err != nil {
		return provider.Result{}, rejected(provider.CodeConfiguration, "codex_cli schema workspace unavailable")
	}
	command := isolatedCommand(ctx, file, cwd, environment, childArguments(call.Model, cwd, schemaPath))
	command.Stdin = bytes.NewReader(compiled.prompt)
	output := &stream{maxBytes: adapter.maxBytes, cancel: cancel}
	stderr := &boundedStderr{max: 16 << 10, cancel: cancel}
	command.Stdout = output
	command.Stderr = stderr
	if err := ctx.Err(); err != nil {
		return provider.Result{}, provider.NewPreDispatchContextError(err)
	}
	if err := observer.BeforePossibleWrite(ctx); err != nil {
		return provider.Result{}, provider.NewError(provider.CodeStateUnavailable, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "codex_cli durable dispatch authorization failed")
	}
	if err := command.Start(); err != nil {
		return provider.Result{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "codex_cli process could not start")
	}
	err = command.Wait()
	if err != nil || ctx.Err() != nil || stderr.overflow || output.finish() != nil {
		// A child exit/error does not prove provider rejection. Never replay,
		// even if the stderr happened to contain an authentication diagnostic.
		diagnostic := strings.ToLower(string(stderr.data))
		if strings.Contains(diagnostic, "auth") || strings.Contains(diagnostic, "refresh token") || strings.Contains(diagnostic, "401") {
			return provider.Result{}, provider.NewError(provider.CodeAuthentication, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "codex_cli authentication failed; operator must restore official ChatGPT login; dispatch cannot be replayed")
		}
		return provider.Result{}, uncertain("codex_cli execution did not produce a valid terminal receipt; do not replay")
	}
	items, status, err := compiled.lift(output.final)
	if err != nil {
		return provider.Result{}, provider.NewError(provider.CodeProviderInvalidResponse, provider.PhaseLift, provider.DispatchAccepted, provider.RetryNever, "codex_cli final response violated the requested JSON or virtual tool contract")
	}
	if err := observer.AfterResponseHeaders(ctx, provider.ResponseMetadata{ProviderTier: "subscription"}); err != nil {
		return provider.Result{}, uncertain("codex_cli terminal receipt could not be persisted; do not replay")
	}
	facts := map[string]json.RawMessage{
		"transport": json.RawMessage(`"codex_cli"`), "billing_mode": json.RawMessage(`"subscription_included"`),
		"cost_provenance":   json.RawMessage(`"catalog_estimate_not_provider_invoice"`),
		"output_limit":      json.RawMessage(`"duration_and_bytes_only_no_hard_token_cap"`),
		"tool_call_mode":    json.RawMessage(`"validated_json_emulation"`),
		"cli_version":       json.RawMessage(strconv.Quote(Version)),
		"thread_id":         json.RawMessage(strconv.Quote(output.threadID)),
		"executable_sha256": json.RawMessage(strconv.Quote(adapter.config.ExecutableSHA256)),
	}
	return provider.Result{Response: llm.Response{APIVersion: llm.APIVersion, OperationKey: call.OperationKey, Status: status, Output: items, Usage: output.usage,
		Route:    llm.RouteFacts{ModelIdentityBasis: llm.ModelIdentityBasisUnknown},
		Provider: llm.ProviderFacts{FinishReason: "turn.completed", Raw: facts}}}, nil
}
