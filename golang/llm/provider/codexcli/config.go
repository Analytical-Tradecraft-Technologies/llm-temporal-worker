// Package codexcli implements private, operator-approved subscription inference
// using the official Codex CLI. It never reads credentials or speaks OAuth.
package codexcli

import (
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
)

const Version = "0.146.1"
const Transport = "codex_cli"

// Config is an explicit approval for one private workflow, not a public job pool.
// Operation keys, when supplied, further restrict the approved root workflow.
// BudgetIsEstimate acknowledges that Codex has no hard output-token control.
type Config struct {
	PrivateOperatorMode   bool     `yaml:"private_operator_mode" json:"private_operator_mode"`
	Executable            string   `yaml:"executable" json:"executable"`
	ExecutableSHA256      string   `yaml:"executable_sha256" json:"executable_sha256"`
	Model                 string   `yaml:"model" json:"model"`
	AuthHome              string   `yaml:"auth_home" json:"auth_home"`
	TempRoot              string   `yaml:"temp_root" json:"temp_root"`
	ApprovedTenant        string   `yaml:"approved_tenant" json:"approved_tenant"`
	ApprovedRootRunID     string   `yaml:"approved_root_run_id" json:"approved_root_run_id"`
	ApprovedOperationKeys []string `yaml:"approved_operation_keys,omitempty" json:"approved_operation_keys,omitempty"`
	BudgetIsEstimate      bool     `yaml:"budget_is_estimate" json:"budget_is_estimate"`
}

func (config Config) Validate() error {
	if !config.PrivateOperatorMode || !config.BudgetIsEstimate {
		return fmt.Errorf("codex_cli requires private_operator_mode and budget_is_estimate acknowledgments")
	}
	for _, path := range []string{config.Executable, config.AuthHome, config.TempRoot} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("codex_cli executable, auth_home and temp_root must be clean absolute paths")
		}
	}
	digest, err := hex.DecodeString(config.ExecutableSHA256)
	if err != nil || len(digest) != 32 || strings.ToLower(config.ExecutableSHA256) != config.ExecutableSHA256 {
		return fmt.Errorf("codex_cli requires a lowercase executable SHA-256 digest")
	}
	for _, value := range []string{config.Model, config.ApprovedTenant, config.ApprovedRootRunID} {
		if strings.TrimSpace(value) != value || value == "" || strings.ContainsAny(value, "\x00\r\n*") {
			return fmt.Errorf("codex_cli requires exact model, approved tenant and root run identities")
		}
	}
	seen := make(map[string]bool, len(config.ApprovedOperationKeys))
	for _, key := range config.ApprovedOperationKeys {
		if key == "" || strings.ContainsAny(key, "\x00\r\n*") || seen[key] {
			return fmt.Errorf("codex_cli operation approvals must be unique exact keys")
		}
		seen[key] = true
	}
	return nil
}
