package config

import (
	"fmt"
	"regexp"
	"strings"
)

// CloudRequestConfig enables durable request/response recording around an
// explicitly composed V1 runtime. It does not configure checkpoint/cache ports.
type CloudRequestConfig struct {
	Provider     CloudStorageProviderConfig `yaml:"provider" json:"provider"`
	RequestTable string                     `yaml:"request_table" json:"request_table"`
	PayloadStore string                     `yaml:"payload_store" json:"payload_store"`
	Namespace    string                     `yaml:"namespace" json:"namespace"`
	// Secret resolves to standard base64 encoding of an independent 32-byte key.
	Secret SecretRef `yaml:"secret" json:"secret"`
}

// CloudStorageProviderConfig mirrors the portable provider factory's JSON
// shape. Typed fields keep credentials and unknown options out of snapshots.
type CloudStorageProviderConfig struct {
	Type           string                `yaml:"type" json:"type"`
	AWS            CloudStorageAWSConfig `yaml:"aws" json:"aws"`
	KeyValueStores map[string]string     `yaml:"key_value_stores" json:"key_value_stores"`
	BlobStores     map[string]string     `yaml:"blob_stores" json:"blob_stores"`
}

type CloudStorageAWSConfig struct {
	Region        string `yaml:"region" json:"region"`
	Profile       string `yaml:"profile,omitempty" json:"profile,omitempty"`
	TempDirectory string `yaml:"temp_directory,omitempty" json:"temp_directory,omitempty"`
}

var cloudNamespacePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func (c CloudRequestConfig) validate() error {
	if c.Provider.Type != "aws" {
		return fmt.Errorf("state.requests.provider.type must be aws")
	}
	if strings.TrimSpace(c.Provider.AWS.Region) == "" {
		return fmt.Errorf("state.requests.provider.aws.region is required")
	}
	if !cloudNamespacePattern.MatchString(c.Namespace) {
		return fmt.Errorf("state.requests.namespace must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}")
	}
	for _, mapping := range []map[string]string{c.Provider.KeyValueStores, c.Provider.BlobStores} {
		for alias, physical := range mapping {
			if !cloudNamespacePattern.MatchString(alias) || strings.TrimSpace(physical) != physical || physical == "" || strings.ContainsAny(physical, "\r\n\x00") {
				return fmt.Errorf("state.requests store mapping is invalid")
			}
		}
	}
	if c.Provider.KeyValueStores[c.RequestTable] == "" {
		return fmt.Errorf("state.requests.request_table must name a configured key_value_stores alias")
	}
	if c.Provider.BlobStores[c.PayloadStore] == "" {
		return fmt.Errorf("state.requests.payload_store must name a configured blob_stores alias")
	}
	if c.Secret.Kind == SecretWorkloadIdentity {
		return fmt.Errorf("state.requests.secret must be a stable file or environment secret")
	}
	return c.Secret.Validate("state.requests.secret")
}
