package config

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	yaml "go.yaml.in/yaml/v4"
)

const APIVersion = "llm-temporal-worker/v1"

// Duration is a bounded configuration duration encoded as a human-readable
// YAML/JSON string (for example, 45s or 2m).
type Duration time.Duration

func (duration Duration) String() string { return time.Duration(duration).String() }

func (duration Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(duration).String())
}

func (duration *Duration) UnmarshalYAML(node *yaml.Node) error {
	if duration == nil {
		return fmt.Errorf("duration target is nil")
	}
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fmt.Errorf("duration must be a string")
	}
	parsed, err := parseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", node.Value, err)
	}
	*duration = Duration(parsed)
	return nil
}

func (duration *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("duration must be a string: %w", err)
	}
	parsed, err := parseDuration(value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value, err)
	}
	*duration = Duration(parsed)
	return nil
}

func parseDuration(value string) (time.Duration, error) {
	if strings.HasSuffix(value, "d") {
		days, err := strconv.ParseInt(strings.TrimSuffix(value, "d"), 10, 64)
		if err != nil || days <= 0 {
			return 0, fmt.Errorf("duration days must be a positive integer")
		}
		const hoursPerDay = int64(24)
		if days > int64((time.Duration(1<<63-1))/time.Hour)/hoursPerDay {
			return 0, fmt.Errorf("duration overflows time.Duration")
		}
		return time.Duration(days*hoursPerDay) * time.Hour, nil
	}
	return time.ParseDuration(value)
}

type Config struct {
	Version      string                    `yaml:"version" json:"version"`
	Environment  string                    `yaml:"environment" json:"environment"`
	Server       ServerConfig              `yaml:"server" json:"server"`
	Temporal     TemporalConfig            `yaml:"temporal" json:"temporal"`
	State        StateConfig               `yaml:"state" json:"state"`
	BlobStore    BlobStoreConfig           `yaml:"blob_store" json:"blob_store"`
	Limits       LimitsConfig              `yaml:"limits" json:"limits"`
	Endpoints    map[string]EndpointConfig `yaml:"endpoints" json:"endpoints"`
	Models       map[string]ModelConfig    `yaml:"models" json:"models"`
	Capabilities CapabilityConfig          `yaml:"capabilities" json:"capabilities"`
	Pricing      PricingConfig             `yaml:"pricing" json:"pricing"`
	Budgets      BudgetsConfig             `yaml:"budgets" json:"budgets"`
	Continuation ContinuationConfig        `yaml:"continuation" json:"continuation"`
	Telemetry    TelemetryConfig           `yaml:"telemetry" json:"telemetry"`
}

type ServerConfig struct {
	HealthAddress          string   `yaml:"health_address" json:"health_address"`
	MetricsAddress         string   `yaml:"metrics_address" json:"metrics_address"`
	ShutdownTimeout        Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`
	FinalizationTimeout    Duration `yaml:"finalization_timeout" json:"finalization_timeout"`
	ReadinessProbeInterval Duration `yaml:"readiness_probe_interval" json:"readiness_probe_interval"`
	ReadinessProbeTimeout  Duration `yaml:"readiness_probe_timeout" json:"readiness_probe_timeout"`
	InlinePayloadBytes     int      `yaml:"inline_payload_bytes" json:"inline_payload_bytes"`
}

type TemporalConfig struct {
	Target         string               `yaml:"target" json:"target"`
	Namespace      string               `yaml:"namespace" json:"namespace"`
	TaskQueue      string               `yaml:"task_queue" json:"task_queue"`
	IdentityPrefix string               `yaml:"identity_prefix" json:"identity_prefix"`
	TLS            TLSConfig            `yaml:"tls" json:"tls"`
	Worker         TemporalWorkerConfig `yaml:"worker" json:"worker"`
}

type TLSConfig struct {
	Enabled    bool   `yaml:"enabled" json:"enabled"`
	ServerName string `yaml:"server_name" json:"server_name"`
	CAFile     string `yaml:"ca_file" json:"ca_file"`
}

type TemporalWorkerConfig struct {
	MaxConcurrentActivities        int      `yaml:"max_concurrent_activities" json:"max_concurrent_activities"`
	MaxConcurrentActivityTaskPolls int      `yaml:"max_concurrent_activity_task_polls" json:"max_concurrent_activity_task_polls"`
	GracefulStopTimeout            Duration `yaml:"graceful_stop_timeout" json:"graceful_stop_timeout"`
	HeartbeatKeepaliveInterval     Duration `yaml:"heartbeat_keepalive_interval" json:"heartbeat_keepalive_interval"`
}

type StateConfig struct {
	Kind                       string         `yaml:"kind" json:"kind"`
	OperationTerminalRetention Duration       `yaml:"operation_terminal_retention" json:"operation_terminal_retention"`
	AmbiguousRetention         Duration       `yaml:"ambiguous_retention" json:"ambiguous_retention"`
	ContinuationRetention      Duration       `yaml:"continuation_retention" json:"continuation_retention"`
	ReservationLease           Duration       `yaml:"reservation_lease" json:"reservation_lease"`
	Redis                      RedisConfig    `yaml:"redis" json:"redis"`
	Postgres                   PostgresConfig `yaml:"postgres" json:"postgres"`
}

// StateKind identifies the storage composition selected for a worker.
// Durable mode is the production composition: PostgreSQL is authoritative
// and Redis provides the low-latency active-budget materialization. Redis-only
// remains accepted for the pre-composition development fixture; new
// production configurations must use Durable.
const (
	StateKindDurable = "durable"
	StateKindMemory  = "memory"
	StateKindRedis   = "redis"
)

type RedisConfig struct {
	Addresses           []string  `yaml:"addresses" json:"addresses"`
	KeyPrefix           string    `yaml:"key_prefix" json:"key_prefix"`
	Username            SecretRef `yaml:"username" json:"username"`
	Password            SecretRef `yaml:"password" json:"password"`
	TLS                 TLSConfig `yaml:"tls" json:"tls"`
	AdmissionHashTag    string    `yaml:"admission_hash_tag" json:"admission_hash_tag"`
	AdmissionMode       string    `yaml:"admission_mode" json:"admission_mode"`
	FunctionLibrary     string    `yaml:"function_library" json:"function_library"`
	AdmissionVersion    string    `yaml:"admission_version" json:"admission_version"`
	AdmissionDigest     string    `yaml:"admission_digest" json:"admission_digest"`
	MaxConnections      int       `yaml:"max_connections" json:"max_connections"`
	DialTimeout         Duration  `yaml:"dial_timeout" json:"dial_timeout"`
	OperationTimeout    Duration  `yaml:"operation_timeout" json:"operation_timeout"`
	RequiredPersistence string    `yaml:"required_persistence" json:"required_persistence"`
	// CoordinationStreamEnabled controls the optional cross-replica wake-up
	// stream. A nil value is normalized by config.Load: durable deployments
	// default to enabled while memory/Redis fixtures remain unchanged.
	CoordinationStreamEnabled *bool `yaml:"coordination_stream_enabled" json:"coordination_stream_enabled"`
	// StreamTrimSafety is the minimum age that a deleted Stream entry must
	// have before readiness accepts the stream's retention state. It is only
	// used when CoordinationStreamEnabled is true.
	StreamTrimSafety Duration `yaml:"stream_trim_safety" json:"stream_trim_safety"`
}

// PostgresConfig selects the authoritative durable-state namespace. Database,
// schema, and table_prefix are deliberately independent so deployments can
// isolate workers without relying on search_path.
type PostgresConfig struct {
	Addresses   []string  `yaml:"addresses" json:"addresses"`
	Database    string    `yaml:"database" json:"database"`
	Schema      string    `yaml:"schema" json:"schema"`
	TablePrefix string    `yaml:"table_prefix" json:"table_prefix"`
	Username    SecretRef `yaml:"username" json:"username"`
	Password    SecretRef `yaml:"password" json:"password"`
	TLS         TLSConfig `yaml:"tls" json:"tls"`
	// MinConnections keeps a bounded warm pool without making connection
	// establishment part of the readiness contract. Zero is valid and lets
	// pgx grow the pool on demand.
	MinConnections         int      `yaml:"min_connections" json:"min_connections"`
	MaxConnections         int      `yaml:"max_connections" json:"max_connections"`
	DialTimeout            Duration `yaml:"dial_timeout" json:"dial_timeout"`
	StatementTimeout       Duration `yaml:"statement_timeout" json:"statement_timeout"`
	LockTimeout            Duration `yaml:"lock_timeout" json:"lock_timeout"`
	IdleTransactionTimeout Duration `yaml:"idle_transaction_timeout" json:"idle_transaction_timeout"`
}

type BlobStoreConfig struct {
	Kind        string         `yaml:"kind" json:"kind"`
	InlineBytes int            `yaml:"inline_bytes" json:"inline_bytes"`
	File        FileBlobConfig `yaml:"file" json:"file"`
	S3          S3Config       `yaml:"s3" json:"s3"`
}

// FileBlobConfig is the development-only durable file-store configuration.
// Production deployments must use a remote object store so results survive a
// pod replacement without relying on a node-local volume.
type FileBlobConfig struct {
	Root string `yaml:"root" json:"root"`
}

type S3Config struct {
	Bucket string     `yaml:"bucket" json:"bucket"`
	Region string     `yaml:"region" json:"region"`
	Prefix string     `yaml:"prefix" json:"prefix"`
	Auth   AuthConfig `yaml:"auth" json:"auth"`
}

type LimitsConfig struct {
	RequestBytes              int      `yaml:"request_bytes" json:"request_bytes"`
	Items                     int      `yaml:"items" json:"items"`
	PartsPerItem              int      `yaml:"parts_per_item" json:"parts_per_item"`
	Tools                     int      `yaml:"tools" json:"tools"`
	SchemaBytes               int      `yaml:"schema_bytes" json:"schema_bytes"`
	JSONDepth                 int      `yaml:"json_depth" json:"json_depth"`
	ContinuationDepth         int      `yaml:"continuation_depth" json:"continuation_depth"`
	RouteAttempts             int      `yaml:"route_attempts" json:"route_attempts"`
	ProviderTimeout           Duration `yaml:"provider_timeout" json:"provider_timeout"`
	ProviderResponseBytes     int64    `yaml:"provider_response_bytes" json:"provider_response_bytes"`
	MaxOutputTokens           int      `yaml:"max_output_tokens" json:"max_output_tokens"`
	MaxBudgetBucketsPerWindow int      `yaml:"max_budget_buckets_per_window" json:"max_budget_buckets_per_window"`
	TokenEstimateSafetyRatio  string   `yaml:"token_estimate_safety_ratio" json:"token_estimate_safety_ratio"`
}

const (
	DefaultProviderResponseBytes int64 = 16 << 20
	MaxProviderResponseBytes     int64 = 64 << 20
)

type EndpointConfig struct {
	Family            string                          `yaml:"family" json:"family"`
	BaseURL           string                          `yaml:"base_url" json:"base_url"`
	OutboundHosts     []string                        `yaml:"outbound_hosts" json:"outbound_hosts"`
	Region            string                          `yaml:"region" json:"region"`
	AWSWorkspaceID    string                          `yaml:"aws_workspace_id" json:"aws_workspace_id"`
	Auth              AuthConfig                      `yaml:"auth" json:"auth"`
	AccountRegion     string                          `yaml:"account_region" json:"account_region"`
	Timeout           Duration                        `yaml:"timeout" json:"timeout"`
	ServiceClasses    map[llm.ServiceClass]TierConfig `yaml:"service_classes" json:"service_classes"`
	CapabilityProfile string                          `yaml:"capability_profile" json:"capability_profile"`
	PriceCatalog      string                          `yaml:"price_catalog" json:"price_catalog"`
	ProviderStorage   ProviderStorageConfig           `yaml:"provider_storage" json:"provider_storage"`
	Extensions        map[string]map[string]any       `yaml:"extensions" json:"extensions"`
}

type TierConfig struct {
	ProviderValue      string `yaml:"provider_value" json:"provider_value"`
	RequiresCapability string `yaml:"requires_capability" json:"requires_capability"`
}

type ProviderStorageConfig struct {
	Permitted bool `yaml:"permitted" json:"permitted"`
}

type AuthConfig struct {
	Kind     string `yaml:"kind" json:"kind"`
	Name     string `yaml:"name" json:"name"`
	Path     string `yaml:"path" json:"path"`
	Audience string `yaml:"audience" json:"audience"`
}

type ModelConfig struct {
	AllowedTenants []string      `yaml:"allowed_tenants" json:"allowed_tenants"`
	DataRegions    []string      `yaml:"data_regions" json:"data_regions"`
	Routes         []RouteConfig `yaml:"routes" json:"routes"`
}

type RouteConfig struct {
	ID       string             `yaml:"id" json:"id"`
	Endpoint string             `yaml:"endpoint" json:"endpoint"`
	Model    string             `yaml:"model" json:"model"`
	Classes  []llm.ServiceClass `yaml:"classes" json:"classes"`
}

type CapabilityConfig struct {
	Catalogs            []CatalogRef `yaml:"catalogs" json:"catalogs"`
	UnknownInStrictMode string       `yaml:"unknown_in_strict_mode" json:"unknown_in_strict_mode"`
}

type PricingConfig struct {
	Catalogs                 []CatalogRef `yaml:"catalogs" json:"catalogs"`
	RequirePriceWhenBudgeted bool         `yaml:"require_price_when_budgeted" json:"require_price_when_budgeted"`
}

type CatalogRef struct {
	File   string `yaml:"file" json:"file"`
	SHA256 string `yaml:"sha256" json:"sha256"`
}

type BudgetsConfig struct {
	RequireMatch bool           `yaml:"require_match" json:"require_match"`
	Policies     []BudgetPolicy `yaml:"policies" json:"policies"`
}

type BudgetPolicy struct {
	ID      string         `yaml:"id" json:"id"`
	Match   BudgetMatch    `yaml:"match" json:"match"`
	Windows []BudgetWindow `yaml:"windows" json:"windows"`
}

type BudgetMatch struct {
	Tenant       string           `yaml:"tenant" json:"tenant,omitempty"`
	Project      string           `yaml:"project" json:"project,omitempty"`
	ActorPrefix  string           `yaml:"actor_prefix" json:"actor_prefix,omitempty"`
	Environment  string           `yaml:"environment" json:"environment,omitempty"`
	LogicalModel string           `yaml:"logical_model" json:"logical_model,omitempty"`
	EndpointID   string           `yaml:"endpoint" json:"endpoint,omitempty"`
	ServiceClass llm.ServiceClass `yaml:"service_class" json:"service_class,omitempty"`
}

type BudgetWindow struct {
	Duration      Duration    `yaml:"duration" json:"duration"`
	Bucket        Duration    `yaml:"bucket" json:"bucket"`
	LimitUSD      pricing.USD `yaml:"limit_usd" json:"limit_usd"`
	LimitMicroUSD int64       `yaml:"limit_micro_usd" json:"limit_micro_usd,omitempty"`
}

type ContinuationConfig struct {
	HandleKeys                []HandleKey `yaml:"handle_keys" json:"handle_keys"`
	RetainCanonicalTranscript bool        `yaml:"retain_canonical_transcript" json:"retain_canonical_transcript"`
	AllowProviderHostedState  bool        `yaml:"allow_provider_hosted_state" json:"allow_provider_hosted_state"`
}

type HandleKey struct {
	ID      string    `yaml:"id" json:"id"`
	Primary bool      `yaml:"primary" json:"primary"`
	Secret  SecretRef `yaml:"secret" json:"secret"`
}

type TelemetryConfig struct {
	Logs           LogConfig     `yaml:"logs" json:"logs"`
	Metrics        MetricsConfig `yaml:"metrics" json:"metrics"`
	Tracing        TracingConfig `yaml:"tracing" json:"tracing"`
	ContentLogging string        `yaml:"content_logging" json:"content_logging"`
}

type LogConfig struct {
	Format string `yaml:"format" json:"format"`
	Level  string `yaml:"level" json:"level"`
}

type MetricsConfig struct {
	Enabled bool `yaml:"enabled" json:"enabled"`
}

type TracingConfig struct {
	Enabled      bool   `yaml:"enabled" json:"enabled"`
	OTLPEndpoint string `yaml:"otlp_endpoint" json:"otlp_endpoint"`
	SampleRatio  string `yaml:"sample_ratio" json:"sample_ratio"`
}

type SecretKind string

const (
	SecretEnv              SecretKind = "env"
	SecretFile             SecretKind = "file"
	SecretWorkloadIdentity SecretKind = "workload_identity"
)

type SecretRef struct {
	Kind     SecretKind `yaml:"kind" json:"kind"`
	Name     string     `yaml:"name" json:"name"`
	Path     string     `yaml:"path" json:"path"`
	Audience string     `yaml:"audience" json:"audience"`
}

var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (secret SecretRef) Validate(path string) error {
	switch secret.Kind {
	case SecretEnv:
		if !envNamePattern.MatchString(secret.Name) || secret.Path != "" || secret.Audience != "" {
			return fmt.Errorf("%s env secret requires a valid name and no path/audience", path)
		}
	case SecretFile:
		if secret.Name != "" || secret.Audience != "" || !filepath.IsAbs(secret.Path) {
			return fmt.Errorf("%s file secret requires an absolute path and no name/audience", path)
		}
	case SecretWorkloadIdentity:
		if secret.Name != "" || secret.Path != "" || secret.Audience == "" {
			return fmt.Errorf("%s workload identity requires audience and no name/path", path)
		}
	default:
		return fmt.Errorf("%s has unknown secret kind %q", path, secret.Kind)
	}
	return nil
}

func (auth AuthConfig) Validate(path string) error {
	switch auth.Kind {
	case "bearer_env", "header_env":
		if !envNamePattern.MatchString(auth.Name) || auth.Path != "" || auth.Audience != "" {
			return fmt.Errorf("%s %s requires a valid environment name", path, auth.Kind)
		}
	case "azure_default_credential", "aws_default_chain":
		if auth.Name != "" || auth.Path != "" || auth.Audience != "" {
			return fmt.Errorf("%s %s does not accept name/path/audience", path, auth.Kind)
		}
	case "workload_identity":
		if auth.Name != "" || auth.Path != "" || auth.Audience == "" {
			return fmt.Errorf("%s workload_identity requires audience", path)
		}
	default:
		return fmt.Errorf("%s has unknown auth kind %q", path, auth.Kind)
	}
	return nil
}

func (config Config) Clone() Config {
	encoded, err := json.Marshal(config)
	if err != nil {
		return Config{}
	}
	var clone Config
	if err := json.Unmarshal(encoded, &clone); err != nil {
		return Config{}
	}
	return clone
}

func (config Config) canonicalJSON() ([]byte, error) {
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	return llm.CanonicalJSON(encoded)
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// NormalizeOutboundHost canonicalizes one explicit provider hostname. Provider
// endpoints must name DNS hosts rather than literal addresses so the runtime
// can resolve and apply its IP-address egress policy before each connection.
func NormalizeOutboundHost(value string) (string, error) {
	host := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(value)), ".")
	if host == "" {
		return "", fmt.Errorf("hostname is required")
	}
	if len(host) > 253 || net.ParseIP(host) != nil {
		return "", fmt.Errorf("hostname must be a DNS name")
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 {
			return "", fmt.Errorf("hostname labels must be between 1 and 63 characters")
		}
		for index, character := range label {
			if character > 127 || !(character == '-' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9') {
				return "", fmt.Errorf("hostname labels must contain only lowercase ASCII letters, digits, or hyphens")
			}
			if (index == 0 || index == len(label)-1) && character == '-' {
				return "", fmt.Errorf("hostname labels must not begin or end with a hyphen")
			}
		}
	}
	return host, nil
}

func normalizedHTTPSURLHost(value, path string, allowEmpty bool) (string, error) {
	if value == "" && allowEmpty {
		return "", nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("%s must be an https URL without userinfo/query/fragment", path)
	}
	if port := parsed.Port(); port != "" {
		parsedPort, parseErr := strconv.ParseUint(port, 10, 16)
		if parseErr != nil || parsedPort == 0 {
			return "", fmt.Errorf("%s must use a valid HTTPS port", path)
		}
	}
	host, err := NormalizeOutboundHost(parsed.Hostname())
	if err != nil {
		return "", fmt.Errorf("%s must name a DNS hostname", path)
	}
	return host, nil
}

func validatePositiveDuration(value Duration, path string) error {
	if time.Duration(value) <= 0 {
		return fmt.Errorf("%s must be positive", path)
	}
	return nil
}

func validateIdentifier(value, path string) error {
	if strings.TrimSpace(value) == "" || strings.ContainsAny(value, " \t\r\n") {
		return fmt.Errorf("%s must be a non-empty identifier", path)
	}
	return nil
}
