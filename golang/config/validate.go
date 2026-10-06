package config

import (
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"net"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

var supportedFamilies = map[string]struct{}{
	"openai_responses":           {},
	"azure_openai_responses":     {},
	"azure_openai_chat":          {},
	"openai_chat":                {},
	"anthropic_messages":         {},
	"anthropic_aws_messages":     {},
	"bedrock_anthropic_messages": {},
	"bedrock_converse":           {},
}

var redisKeyPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

const maxAdmissionFieldBytes = 256

// Validate checks references, closed enums, safety bounds, and retention
// inequalities. It never resolves secret values or performs network I/O.
func (config Config) Validate() error {
	if config.Langfuse != nil {
		if err := config.Langfuse.validate(); err != nil {
			return err
		}
	}
	if config.Version != APIVersion {
		return fmt.Errorf("version must be %q", APIVersion)
	}
	if err := validateIdentifier(config.Environment, "environment"); err != nil {
		return err
	}
	if err := config.Server.validate(); err != nil {
		return err
	}
	if err := config.Temporal.validate(config.Environment); err != nil {
		return err
	}
	if config.Authorization != nil {
		if err := config.Authorization.Validate(); err != nil {
			return err
		}
	}
	if err := validateShutdownBudget(config.Server, config.Temporal.Worker); err != nil {
		return err
	}
	if err := config.State.validate(config.Environment); err != nil {
		return err
	}
	if err := config.BlobStore.validate(config.Environment); err != nil {
		return err
	}
	if err := validateStateBlobComposition(config.State.Kind, config.BlobStore.Kind); err != nil {
		return err
	}
	if err := config.Limits.validate(); err != nil {
		return err
	}
	if len(config.Endpoints) == 0 {
		return fmt.Errorf("endpoints must not be empty")
	}
	for _, name := range sortedKeys(config.Endpoints) {
		if len(name) > maxAdmissionFieldBytes {
			return fmt.Errorf("endpoints.%s exceeds the %d-byte admission field limit", name, maxAdmissionFieldBytes)
		}
		// An endpoint that only model_sync uses is priced from the synced
		// catalog, so it does not need a configured price catalog.
		priceCatalogOptional := config.ModelSyncEndpoint(name) && !config.RoutedEndpoint(name)
		if err := config.Endpoints[name].validate("endpoints."+name, config.Limits.ProviderTimeout, priceCatalogOptional); err != nil {
			return err
		}
		if config.Endpoints[name].Optional {
			if !config.ModelSyncEndpoint(name) || config.RoutedEndpoint(name) {
				return fmt.Errorf("endpoints.%s.optional is only valid for an endpoint that model_sync uses and no models route references", name)
			}
			if kind := config.Endpoints[name].Auth.Kind; kind != "bearer_env" && kind != "header_env" {
				return fmt.Errorf("endpoints.%s.optional requires bearer_env or header_env auth", name)
			}
		}
	}
	if len(config.Models) == 0 && config.ModelSync == nil {
		return fmt.Errorf("models must not be empty")
	}
	for _, name := range sortedKeys(config.Models) {
		if err := config.Models[name].validate("models."+name, config.Endpoints); err != nil {
			return err
		}
	}
	if err := config.Capabilities.validate(); err != nil {
		return err
	}
	if err := config.Pricing.validate(config.ModelSync != nil); err != nil {
		return err
	}
	if config.ModelSync != nil {
		if err := config.ModelSync.validate(config.Endpoints); err != nil {
			return err
		}
	}
	if err := config.Budgets.validate(); err != nil {
		return err
	}
	if err := config.validateBudgetReferences(); err != nil {
		return err
	}
	if err := config.Continuation.validate(); err != nil {
		return err
	}
	if err := config.Telemetry.validate(config.Environment); err != nil {
		return err
	}
	return nil
}

// validateShutdownBudget keeps the process shutdown deadline longer than the
// two bounded phases that can still need to run after polling stops. Temporal's
// worker stop waits for in-flight Activities up to graceful_stop_timeout;
// finalization_timeout bounds the short state/result reconciliation writes that
// protect an accepted provider call. Equality is rejected because it leaves no
// budget for closing clients and flushing telemetry.
func validateShutdownBudget(server ServerConfig, worker TemporalWorkerConfig) error {
	shutdown := time.Duration(server.ShutdownTimeout)
	gracefulStop := time.Duration(worker.GracefulStopTimeout)
	finalization := time.Duration(server.FinalizationTimeout)
	if shutdown <= 0 || gracefulStop <= 0 || finalization <= 0 {
		// The individual validators produce the more specific configuration
		// errors. Avoid reporting a misleading ordering error before they run.
		return nil
	}
	if shutdown <= gracefulStop || shutdown-gracefulStop <= finalization {
		return fmt.Errorf("server.shutdown_timeout must exceed temporal.worker.graceful_stop_timeout + server.finalization_timeout")
	}
	return nil
}

func validateStateBlobComposition(stateKind, blobKind string) error {
	if stateKind == StateKindMemory && blobKind != "memory" {
		return fmt.Errorf("blob_store.kind must be memory when state.kind is memory")
	}
	if stateKind != StateKindMemory && blobKind == "memory" {
		return fmt.Errorf("blob_store.kind memory requires state.kind memory")
	}
	return nil
}

func (server ServerConfig) validate() error {
	if err := validateAddress(server.HealthAddress, "server.health_address"); err != nil {
		return err
	}
	if err := validateAddress(server.MetricsAddress, "server.metrics_address"); err != nil {
		return err
	}
	// The listener is shared only when the two strings are identical; any
	// other spelling of the same port (":8080" and "0.0.0.0:8080") would make
	// the second bind fail at startup.
	if server.HealthAddress != server.MetricsAddress {
		_, healthText, _ := net.SplitHostPort(server.HealthAddress)
		_, metricsText, _ := net.SplitHostPort(server.MetricsAddress)
		// Compare numeric ports: "8080" and "08080" bind the same port.
		healthPort, _ := strconv.Atoi(healthText)
		metricsPort, _ := strconv.Atoi(metricsText)
		if healthPort == metricsPort && healthPort != 0 {
			return fmt.Errorf("server.health_address and server.metrics_address must be identical to share a listener, or use different ports")
		}
	}
	if err := validatePositiveDuration(server.ShutdownTimeout, "server.shutdown_timeout"); err != nil {
		return err
	}
	if err := validatePositiveDuration(server.FinalizationTimeout, "server.finalization_timeout"); err != nil {
		return err
	}
	if err := validatePositiveDuration(server.ReadinessProbeInterval, "server.readiness_probe_interval"); err != nil {
		return err
	}
	if err := validatePositiveDuration(server.ReadinessProbeTimeout, "server.readiness_probe_timeout"); err != nil {
		return err
	}
	if server.ReadinessProbeTimeout > server.ReadinessProbeInterval {
		return fmt.Errorf("server.readiness_probe_timeout must not exceed readiness_probe_interval")
	}
	if server.InlinePayloadBytes <= 0 || server.InlinePayloadBytes > TemporalBlobLimitBytes {
		return fmt.Errorf("server.inline_payload_bytes must be between 1 and %d (the Temporal payload blob limit)", TemporalBlobLimitBytes)
	}
	return nil
}

func (temporal TemporalConfig) validate(environment string) error {
	if err := temporal.Worker.Versioning.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(temporal.Target) == "" || strings.ContainsAny(temporal.Target, "\r\n") {
		return fmt.Errorf("temporal.target must be non-empty")
	}
	for name, value := range map[string]string{
		"temporal.namespace":       temporal.Namespace,
		"temporal.task_queue":      temporal.TaskQueue,
		"temporal.identity_prefix": temporal.IdentityPrefix,
	} {
		if err := validateIdentifier(value, name); err != nil {
			return err
		}
	}
	if temporal.TLS.Enabled && temporal.TLS.CAFile == "" {
		return fmt.Errorf("temporal.tls.ca_file is required when TLS is enabled")
	}
	if temporal.TLS.Enabled && temporal.TLS.ServerName == "" {
		return fmt.Errorf("temporal.tls.server_name is required when TLS is enabled")
	}
	if (temporal.TLS.CertFile == "") != (temporal.TLS.KeyFile == "") {
		return fmt.Errorf("temporal.tls.cert_file and temporal.tls.key_file must be set together")
	}
	if !temporal.TLS.Enabled && (temporal.TLS.CertFile != "" || temporal.APIKeyFile != "") {
		return fmt.Errorf("temporal client credentials require temporal.tls.enabled")
	}
	// trusted_temporal trusts Temporal to authenticate callers, so production
	// needs an encrypted, authenticated connection to it: worker TLS with a
	// client certificate or API key, or a service mesh that supplies both.
	if IsProductionEnvironment(environment) && !temporal.MeshTransport {
		if !temporal.TLS.Enabled {
			return fmt.Errorf("temporal.tls.enabled must be true in production unless temporal.mesh_transport is set")
		}
		if temporal.TLS.CertFile == "" && temporal.APIKeyFile == "" {
			return fmt.Errorf("temporal.tls.cert_file/key_file or temporal.api_key_file is required in production unless temporal.mesh_transport is set")
		}
	}
	if temporal.PayloadCodec != nil {
		if err := temporal.PayloadCodec.validate(); err != nil {
			return err
		}
	}
	if temporal.Worker.MaxConcurrentActivities <= 0 || temporal.Worker.MaxConcurrentActivityTaskPolls <= 0 {
		return fmt.Errorf("temporal.worker concurrency values must be positive")
	}
	if err := validatePositiveDuration(temporal.Worker.GracefulStopTimeout, "temporal.worker.graceful_stop_timeout"); err != nil {
		return err
	}
	if err := validatePositiveDuration(temporal.Worker.HeartbeatKeepaliveInterval, "temporal.worker.heartbeat_keepalive_interval"); err != nil {
		return err
	}
	if time.Duration(temporal.Worker.HeartbeatKeepaliveInterval) > ActivityHeartbeatTimeout/3 {
		return fmt.Errorf("temporal.worker.heartbeat_keepalive_interval must be at most %s (one third of the %s Activity heartbeat timeout)", ActivityHeartbeatTimeout/3, ActivityHeartbeatTimeout)
	}
	return nil
}

// payloadCodecKeyIDPattern keeps key IDs short and printable: they are
// written, unencrypted, into the metadata of every encoded payload.
var payloadCodecKeyIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func (codec PayloadCodecConfig) validate() error {
	if codec.Kind != PayloadCodecAES256GCM {
		return fmt.Errorf("temporal.payload_codec.kind must be %s", PayloadCodecAES256GCM)
	}
	if len(codec.Keys) == 0 {
		return fmt.Errorf("temporal.payload_codec.keys must not be empty")
	}
	primary := 0
	seen := make(map[string]struct{}, len(codec.Keys))
	for index, key := range codec.Keys {
		path := fmt.Sprintf("temporal.payload_codec.keys[%d]", index)
		if !payloadCodecKeyIDPattern.MatchString(key.ID) {
			return fmt.Errorf("%s.id must be 1-64 characters from [A-Za-z0-9._-]", path)
		}
		if _, exists := seen[key.ID]; exists {
			return fmt.Errorf("%s duplicate key ID %q", path, key.ID)
		}
		seen[key.ID] = struct{}{}
		if key.Primary {
			primary++
		}
		if err := key.Secret.Validate(path + ".secret"); err != nil {
			return err
		}
	}
	if primary != 1 {
		return fmt.Errorf("temporal.payload_codec.keys must contain exactly one primary key")
	}
	return nil
}

func (state StateConfig) validate(environment string) error {
	if state.Requests != nil {
		if state.Kind != StateKindDurable {
			return fmt.Errorf("state.requests requires durable state")
		}
		if err := state.Requests.validate(); err != nil {
			return err
		}
	}
	switch state.Kind {
	case StateKindDurable:
		// Durable mode requires Redis and a durable request backend. Readiness
		// verifies their contracts before a worker can poll new work.
	case StateKindRedis:
		// Kept for the existing local Redis-only fixture while the durable
		// repositories are adopted. It is never accepted as production.
		if IsProductionEnvironment(environment) {
			return fmt.Errorf("state.kind redis is not permitted in production; use durable")
		}
	case StateKindMemory:
		if IsProductionEnvironment(environment) {
			return fmt.Errorf("state.kind memory is not permitted in production; use durable")
		}
	default:
		return fmt.Errorf("state.kind %q is unsupported", state.Kind)
	}
	for name, value := range map[string]Duration{
		"state.operation_terminal_retention": state.OperationTerminalRetention,
		"state.ambiguous_retention":          state.AmbiguousRetention,
		"state.continuation_retention":       state.ContinuationRetention,
		"state.reservation_lease":            state.ReservationLease,
	} {
		if err := validatePositiveDuration(value, name); err != nil {
			return err
		}
	}
	if state.OperationTerminalRetention < state.ReservationLease {
		return fmt.Errorf("state.operation_terminal_retention must cover reservation_lease")
	}
	if state.AmbiguousRetention < state.OperationTerminalRetention {
		return fmt.Errorf("state.ambiguous_retention must cover operation_terminal_retention")
	}
	if state.Kind == StateKindMemory {
		// Memory mode intentionally does not validate, resolve, or dial the
		// external storage sections. Their addresses and credentials are ignored
		// and should normally be omitted from a development configuration.
		return nil
	}
	if err := state.Redis.validate(environment); err != nil {
		return err
	}
	// Durable mode requires cloud request storage. Redis-only remains a
	// development fixture and must not create a substitute durable backend.
	if state.Requests != nil {
		return nil
	}
	if state.Kind == StateKindDurable {
		return fmt.Errorf("state.requests is required for durable state")
	}
	return nil
}

func (redis RedisConfig) validate(environment string) error {
	if !redisKeyPrefixPattern.MatchString(redis.KeyPrefix) {
		return fmt.Errorf("state.redis.key_prefix must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}")
	}
	if redis.TLS.CertFile != "" || redis.TLS.KeyFile != "" {
		return fmt.Errorf("state.redis.tls.cert_file and key_file are not supported")
	}
	if IsProductionEnvironment(environment) && !redis.TLS.Enabled && !redis.ServiceMesh {
		return fmt.Errorf("state.redis.tls.enabled must be true in production unless service_mesh is enabled")
	}
	if len(redis.Addresses) == 0 {
		return fmt.Errorf("state.redis.addresses must not be empty")
	}
	for index, address := range redis.Addresses {
		if err := validateAddress(address, fmt.Sprintf("state.redis.addresses[%d]", index)); err != nil {
			return err
		}
	}
	if redis.ServiceMesh {
		if redis.TLS != (TLSConfig{}) || redis.Username != (SecretRef{}) || redis.Password != (SecretRef{}) {
			return fmt.Errorf("state.redis.service_mesh requires disabled application TLS and no username/password references")
		}
	} else {
		if err := redis.Username.Validate("state.redis.username"); err != nil {
			return err
		}
		if err := redis.Password.Validate("state.redis.password"); err != nil {
			return err
		}
	}
	if err := redis.KeySecret.Validate("state.redis.key_secret"); err != nil {
		return err
	}
	if redis.AdmissionHashTag == "" || redis.FunctionLibrary == "" || redis.AdmissionVersion == "" {
		return fmt.Errorf("state.redis.admission_hash_tag, function_library, and admission_version are required")
	}
	if err := validateAdmissionHashTag(redis.AdmissionHashTag); err != nil {
		return err
	}
	switch redis.AdmissionMode {
	case "function", "lua":
	default:
		return fmt.Errorf("state.redis.admission_mode %q is unsupported", redis.AdmissionMode)
	}
	if len(redis.AdmissionDigest) != 64 {
		return fmt.Errorf("state.redis.admission_digest must be 64 hex characters")
	}
	if _, err := hex.DecodeString(redis.AdmissionDigest); err != nil {
		return fmt.Errorf("state.redis.admission_digest must be hex")
	}
	if redis.MaxConnections <= 0 || redis.MaxConnections > 100000 {
		return fmt.Errorf("state.redis.max_connections is outside safe bounds")
	}
	if err := validatePositiveDuration(redis.DialTimeout, "state.redis.dial_timeout"); err != nil {
		return err
	}
	if err := validatePositiveDuration(redis.OperationTimeout, "state.redis.operation_timeout"); err != nil {
		return err
	}
	switch redis.RequiredPersistence {
	case "aof_and_rdb", "aof", "rdb":
		// A disabled coordination stream is a supported Redis-only fixture
		// mode. Enabled streams require a bounded trim-safety margin so the
		// readiness probe can reject recent destructive trimming.
		if redis.CoordinationStreamEnabled != nil && *redis.CoordinationStreamEnabled {
			if err := validatePositiveDuration(redis.StreamTrimSafety, "state.redis.stream_trim_safety"); err != nil {
				return err
			}
			if redis.StreamTrimSafety < Duration(time.Second) {
				return fmt.Errorf("state.redis.stream_trim_safety must be at least 1s")
			}
			if redis.StreamTrimSafety > Duration(30*24*time.Hour) {
				return fmt.Errorf("state.redis.stream_trim_safety exceeds 30d")
			}
		}
		return nil
	default:
		return fmt.Errorf("state.redis.required_persistence %q is unsupported", redis.RequiredPersistence)
	}
}

func (blob BlobStoreConfig) validate(environment string) error {
	if blob.InlineBytes <= 0 || blob.InlineBytes > 16<<20 {
		return fmt.Errorf("blob_store.inline_bytes is outside safe bounds")
	}
	switch blob.Kind {
	case "memory":
		if IsProductionEnvironment(environment) {
			return fmt.Errorf("blob_store.kind memory is supported only in development")
		}
		if blob.File.Root != "" || blob.S3.Bucket != "" || blob.S3.Region != "" || blob.S3.Prefix != "" || blob.S3.Auth != (AuthConfig{}) || blob.S3.Failover != nil {
			return fmt.Errorf("blob_store.file and blob_store.s3 are not valid when blob_store.kind is memory")
		}
		return nil
	case "s3":
		if blob.File.Root != "" {
			return fmt.Errorf("blob_store.file is only valid when blob_store.kind is file")
		}
		if blob.S3.Bucket == "" || blob.S3.Region == "" || blob.S3.Prefix == "" {
			return fmt.Errorf("blob_store.s3 bucket, region, and prefix are required")
		}
		if f := blob.S3.Failover; f != nil {
			if err := validateRegionalBuckets(blob.S3.Region, blob.S3.Bucket, f.Replicas); err != nil {
				return err
			}
			if err := validateAttemptTimeout(f.AttemptTimeout); err != nil {
				return err
			}
		}
		return blob.S3.Auth.Validate("blob_store.s3.auth")
	case "file":
		if IsProductionEnvironment(environment) {
			return fmt.Errorf("blob_store.kind file is supported only in development")
		}
		root := strings.TrimSpace(blob.File.Root)
		if root == "" || !filepath.IsAbs(root) || strings.ContainsAny(root, "\r\n") {
			return fmt.Errorf("blob_store.file.root must be an absolute path")
		}
		for _, segment := range strings.Split(root, string(filepath.Separator)) {
			if segment == "." || segment == ".." {
				return fmt.Errorf("blob_store.file.root must not contain dot path segments")
			}
		}
		if filepath.Clean(root) == string(filepath.Separator) {
			return fmt.Errorf("blob_store.file.root must not be the filesystem root")
		}
		if blob.S3.Bucket != "" || blob.S3.Region != "" || blob.S3.Prefix != "" || blob.S3.Auth != (AuthConfig{}) || blob.S3.Failover != nil {
			return fmt.Errorf("blob_store.s3 is only valid when blob_store.kind is s3")
		}
		return nil
	default:
		return fmt.Errorf("blob_store.kind %q is unsupported", blob.Kind)
	}
}

func (limits LimitsConfig) validate() error {
	positive := map[string]int{
		"limits.request_bytes":                 limits.RequestBytes,
		"limits.items":                         limits.Items,
		"limits.parts_per_item":                limits.PartsPerItem,
		"limits.tools":                         limits.Tools,
		"limits.schema_bytes":                  limits.SchemaBytes,
		"limits.json_depth":                    limits.JSONDepth,
		"limits.continuation_depth":            limits.ContinuationDepth,
		"limits.route_attempts":                limits.RouteAttempts,
		"limits.max_output_tokens":             limits.MaxOutputTokens,
		"limits.max_budget_buckets_per_window": limits.MaxBudgetBucketsPerWindow,
	}
	for name, value := range positive {
		if value <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	// Request output limits are carried in a signed 32-bit field.
	if limits.MaxOutputTokens > math.MaxInt32 {
		return fmt.Errorf("limits.max_output_tokens must not exceed %d", math.MaxInt32)
	}
	if limits.RequestBytes > 64<<20 || limits.SchemaBytes > limits.RequestBytes {
		return fmt.Errorf("limits request/schema byte bounds are unsafe")
	}
	if err := validatePositiveDuration(limits.ProviderTimeout, "limits.provider_timeout"); err != nil {
		return err
	}
	if time.Duration(limits.ProviderTimeout) >= ActivityStartToClose {
		return fmt.Errorf("limits.provider_timeout must be shorter than the %s Activity start-to-close timeout", ActivityStartToClose)
	}
	if limits.ProviderResponseBytes <= 0 || limits.ProviderResponseBytes > MaxProviderResponseBytes {
		return fmt.Errorf("limits.provider_response_bytes must be between 1 and %d", MaxProviderResponseBytes)
	}
	ratio, ok := new(big.Rat).SetString(limits.TokenEstimateSafetyRatio)
	if !ok || ratio.Sign() <= 0 || ratio.Cmp(big.NewRat(100, 1)) > 0 {
		return fmt.Errorf("limits.token_estimate_safety_ratio must be a finite positive decimal <= 100")
	}
	return nil
}

func (endpoint EndpointConfig) validate(path string, providerTimeout Duration, priceCatalogOptional bool) error {
	if _, ok := supportedFamilies[endpoint.Family]; !ok {
		return fmt.Errorf("%s.family %q is unsupported", path, endpoint.Family)
	}
	// YAML resolves unquoted values such as 2024-10-21 to timestamps, which
	// would later be sent to Azure as 2024-10-21T00:00:00Z.
	for _, field := range []string{"api_version", "deployment"} {
		if value, present := endpoint.Extensions["azure"][field]; present {
			if _, ok := value.(string); !ok {
				return fmt.Errorf("%s.extensions.azure.%s must be a quoted string", path, field)
			}
		}
	}
	baseHost := ""
	var err error
	if endpoint.Family == "bedrock_anthropic_messages" || endpoint.Family == "bedrock_converse" {
		if endpoint.Region == "" {
			return fmt.Errorf("%s.region is required for Bedrock", path)
		}
		if endpoint.Auth.Kind != "aws_default_chain" {
			return fmt.Errorf("%s.auth.kind must be aws_default_chain for Bedrock", path)
		}
		baseHost, err = normalizedHTTPSURLHost(endpoint.BaseURL, path+".base_url", true)
		if err != nil {
			return err
		}
	} else if endpoint.Family == "anthropic_aws_messages" {
		if endpoint.Region == "" {
			return fmt.Errorf("%s.region is required for Anthropic AWS gateway", path)
		}
		if err := validateIdentifier(endpoint.Region, path+".region"); err != nil {
			return err
		}
		if endpoint.AWSWorkspaceID == "" {
			return fmt.Errorf("%s.aws_workspace_id is required for Anthropic AWS gateway", path)
		}
		if err := validateIdentifier(endpoint.AWSWorkspaceID, path+".aws_workspace_id"); err != nil {
			return err
		}
		if endpoint.Auth.Kind != "aws_default_chain" {
			return fmt.Errorf("%s.auth.kind must be aws_default_chain for Anthropic AWS gateway", path)
		}
		baseHost, err = normalizedHTTPSURLHost(endpoint.BaseURL, path+".base_url", false)
		if err != nil {
			return err
		}
	} else if baseHost, err = normalizedHTTPSURLHost(endpoint.BaseURL, path+".base_url", false); err != nil {
		return err
	}
	if endpoint.Family != "anthropic_aws_messages" && endpoint.AWSWorkspaceID != "" {
		return fmt.Errorf("%s.aws_workspace_id is only valid for Anthropic AWS gateway endpoints", path)
	}
	if err := endpoint.validateOutboundHosts(path, baseHost); err != nil {
		return err
	}
	if err := endpoint.Auth.Validate(path + ".auth"); err != nil {
		return err
	}
	if endpoint.AccountRegion == "" && endpoint.Region == "" {
		return fmt.Errorf("%s.account_region or region is required", path)
	}
	if err := validatePositiveDuration(endpoint.Timeout, path+".timeout"); err != nil {
		return err
	}
	if endpoint.Timeout > providerTimeout {
		return fmt.Errorf("%s.timeout must not exceed limits.provider_timeout", path)
	}
	if endpoint.CapabilityProfile == "" || (endpoint.PriceCatalog == "" && !priceCatalogOptional) {
		return fmt.Errorf("%s capability_profile and price_catalog are required", path)
	}
	if len(endpoint.ServiceClasses) == 0 {
		return fmt.Errorf("%s.service_classes must not be empty", path)
	}
	for class, tier := range endpoint.ServiceClasses {
		if !class.Valid() {
			return fmt.Errorf("%s.service_classes contains unknown public class %q; want economy, standard, or priority", path, class)
		}
		if tier.ProviderValue == "" {
			return fmt.Errorf("%s.service_classes.%s.provider_value is required", path, class)
		}
	}
	return nil
}

func (endpoint EndpointConfig) validateOutboundHosts(path, baseHost string) error {
	if len(endpoint.OutboundHosts) == 0 {
		return fmt.Errorf("%s.outbound_hosts must not be empty", path)
	}
	seen := make(map[string]struct{}, len(endpoint.OutboundHosts))
	baseAllowed := baseHost == ""
	for index, rawHost := range endpoint.OutboundHosts {
		host, err := NormalizeOutboundHost(rawHost)
		if err != nil {
			return fmt.Errorf("%s.outbound_hosts[%d] must be a normalized DNS hostname", path, index)
		}
		if _, duplicate := seen[host]; duplicate {
			return fmt.Errorf("%s.outbound_hosts contains duplicate hostname", path)
		}
		seen[host] = struct{}{}
		if host == baseHost {
			baseAllowed = true
		}
	}
	if !baseAllowed {
		return fmt.Errorf("%s.outbound_hosts must include the base_url hostname", path)
	}
	return nil
}

func (model ModelConfig) validate(path string, endpoints map[string]EndpointConfig) error {
	if len(model.Routes) == 0 {
		return fmt.Errorf("%s.routes must not be empty", path)
	}
	seen := make(map[string]struct{}, len(model.Routes))
	for index, route := range model.Routes {
		routePath := fmt.Sprintf("%s.routes[%d]", path, index)
		if err := validateIdentifier(route.ID, routePath+".id"); err != nil {
			return err
		}
		if len(route.ID) > maxAdmissionFieldBytes {
			return fmt.Errorf("%s.id exceeds the %d-byte admission field limit", routePath, maxAdmissionFieldBytes)
		}
		if _, exists := seen[route.ID]; exists {
			return fmt.Errorf("%s duplicate route ID %q", path, route.ID)
		}
		seen[route.ID] = struct{}{}
		endpoint, exists := endpoints[route.Endpoint]
		if !exists {
			return fmt.Errorf("%s.endpoint %q is not configured", routePath, route.Endpoint)
		}
		if route.Model == "" {
			return fmt.Errorf("%s.model is required", routePath)
		}
		if len(route.Model) > maxAdmissionFieldBytes {
			return fmt.Errorf("%s.model exceeds the %d-byte admission field limit", routePath, maxAdmissionFieldBytes)
		}
		if len(route.Classes) == 0 {
			return fmt.Errorf("%s.classes must not be empty", routePath)
		}
		classSeen := make(map[llm.ServiceClass]struct{}, len(route.Classes))
		for _, class := range route.Classes {
			if !class.Valid() {
				return fmt.Errorf("%s.classes contains unknown public class %q", routePath, class)
			}
			if _, duplicate := classSeen[class]; duplicate {
				return fmt.Errorf("%s.classes repeats %q", routePath, class)
			}
			classSeen[class] = struct{}{}
			if _, supported := endpoint.ServiceClasses[class]; !supported {
				return fmt.Errorf("%s class %q is not mapped by endpoint %q", routePath, class, route.Endpoint)
			}
		}
	}
	return nil
}

func (catalogs CapabilityConfig) validate() error {
	// Strict mode always rejects an unknown capability; no "allow" behaviour
	// is implemented, so it is not accepted.
	if catalogs.UnknownInStrictMode != "reject" {
		return fmt.Errorf("capabilities.unknown_in_strict_mode must be reject")
	}
	return validateCatalogs(catalogs.Catalogs, "capabilities.catalogs")
}

func (pricing PricingConfig) validate(modelSync bool) error {
	// A model_sync deployment can be priced entirely from the synced catalog.
	if modelSync && len(pricing.Catalogs) == 0 {
		return nil
	}
	return validateCatalogs(pricing.Catalogs, "pricing.catalogs")
}

// RoutedEndpoint reports whether any configured models route references
// endpointID.
func (config Config) RoutedEndpoint(endpointID string) bool {
	for _, model := range config.Models {
		for _, route := range model.Routes {
			if route.Endpoint == endpointID {
				return true
			}
		}
	}
	return false
}

func (sync ModelSyncConfig) validate(endpoints map[string]EndpointConfig) error {
	const path = "model_sync"
	openrouter, ok := endpoints[sync.OpenRouter.Endpoint]
	if sync.OpenRouter.Endpoint == "" || !ok {
		return fmt.Errorf("%s.openrouter.endpoint %q is not configured", path, sync.OpenRouter.Endpoint)
	}
	if _, marked := openrouter.Extensions["openrouter"]; openrouter.Family != "openai_chat" || !marked {
		return fmt.Errorf("%s.openrouter.endpoint must be an openai_chat endpoint with the openrouter extension", path)
	}
	if order, present := openrouter.Extensions["openrouter"]["provider_order"]; present && order != nil {
		return fmt.Errorf("%s.openrouter.endpoint must not pin provider_order; OpenRouter selects the upstream for synced models", path)
	}
	seen := map[string]struct{}{sync.OpenRouter.Endpoint: {}}
	for index, direct := range sync.Direct {
		directPath := fmt.Sprintf("%s.direct[%d]", path, index)
		if _, ok := endpoints[direct.Endpoint]; direct.Endpoint == "" || !ok {
			return fmt.Errorf("%s.endpoint %q is not configured", directPath, direct.Endpoint)
		}
		if _, duplicate := seen[direct.Endpoint]; duplicate {
			return fmt.Errorf("%s.endpoint %q is already used by model_sync", directPath, direct.Endpoint)
		}
		seen[direct.Endpoint] = struct{}{}
		if err := validateIdentifier(direct.Provider, directPath+".provider"); err != nil {
			return err
		}
	}
	if len(sync.Rules) > 0 {
		if err := validateCatalogs(sync.Rules, path+".rules"); err != nil {
			return err
		}
	}
	minimum, maximum := time.Duration(sync.RefreshIntervalMin), time.Duration(sync.RefreshIntervalMax)
	if minimum < time.Minute || maximum > 24*time.Hour || maximum < minimum {
		return fmt.Errorf("%s refresh interval must satisfy 1m <= refresh_interval_min <= refresh_interval_max <= 24h", path)
	}
	return nil
}

func validateCatalogs(catalogs []CatalogRef, path string) error {
	if len(catalogs) == 0 {
		return fmt.Errorf("%s must not be empty", path)
	}
	for index, catalog := range catalogs {
		if catalog.File == "" || !strings.HasPrefix(catalog.File, "/") {
			return fmt.Errorf("%s[%d].file must be an absolute path", path, index)
		}
		if len(catalog.SHA256) != 64 {
			return fmt.Errorf("%s[%d].sha256 must be 64 hex characters", path, index)
		}
		if _, err := hex.DecodeString(catalog.SHA256); err != nil {
			return fmt.Errorf("%s[%d].sha256 must be hex", path, index)
		}
	}
	return nil
}

func (budgets BudgetsConfig) validate() error {
	seen := make(map[string]struct{}, len(budgets.Policies))
	for index, policy := range budgets.Policies {
		path := fmt.Sprintf("budgets.policies[%d]", index)
		if err := validateIdentifier(policy.ID, path+".id"); err != nil {
			return err
		}
		if _, exists := seen[policy.ID]; exists {
			return fmt.Errorf("%s duplicate policy ID %q", path, policy.ID)
		}
		seen[policy.ID] = struct{}{}
		match := policy.Match
		if !hasBudgetMatchRestriction(match) {
			return fmt.Errorf("%s.match must contain at least one restriction", path)
		}
		if match.ServiceClass != "" && !match.ServiceClass.Valid() {
			return fmt.Errorf("%s.match.service_class must be economy, standard, or priority", path)
		}
		if len(policy.Windows) == 0 {
			return fmt.Errorf("%s.windows must not be empty", path)
		}
		if err := validateBudgetWindowIdentities(policy, path); err != nil {
			return err
		}
		for windowIndex, window := range policy.Windows {
			windowPath := fmt.Sprintf("%s.windows[%d]", path, windowIndex)
			if err := validatePositiveDuration(window.Duration, windowPath+".duration"); err != nil {
				return err
			}
			if err := validatePositiveDuration(window.Bucket, windowPath+".bucket"); err != nil {
				return err
			}
			if window.Bucket > window.Duration {
				return fmt.Errorf("%s.bucket must not exceed duration", windowPath)
			}
			// Redis computes window expiry in whole milliseconds; a
			// fractional-millisecond geometry would shift its bucket grid.
			if time.Duration(window.Duration)%time.Millisecond != 0 || time.Duration(window.Bucket)%time.Millisecond != 0 {
				return fmt.Errorf("%s duration and bucket must be whole milliseconds", windowPath)
			}
			if !window.LimitUSD.IsZero() {
				if err := window.LimitUSD.Validate(); err != nil {
					return fmt.Errorf("%s.limit_usd: %w", windowPath, err)
				}
			} else if window.LimitMicroUSD <= 0 {
				return fmt.Errorf("%s.limit_usd must be positive", windowPath)
			}
			if err := validateBudgetLimit(window, windowPath); err != nil {
				return err
			}
		}
	}
	if budgets.RequireMatch && len(budgets.Policies) == 0 {
		return fmt.Errorf("budgets.policies is required when require_match is true")
	}
	return nil
}

func hasBudgetMatchRestriction(match BudgetMatch) bool {
	return (match.Tenant != "" && match.Tenant != "*") ||
		(match.Project != "" && match.Project != "*") ||
		(match.ActorPrefix != "" && match.ActorPrefix != "*") ||
		(match.Environment != "" && match.Environment != "*") ||
		(match.LogicalModel != "" && match.LogicalModel != "*") ||
		(match.EndpointID != "" && match.EndpointID != "*") ||
		match.ServiceClass != ""
}

func (continuation ContinuationConfig) validate() error {
	// Checkpoints always keep the canonical transcript, which replay and
	// compaction depend on; a setting that claims otherwise is rejected
	// rather than silently ignored.
	if continuation.RetainCanonicalTranscript != nil && !*continuation.RetainCanonicalTranscript {
		return fmt.Errorf("continuation.retain_canonical_transcript must be true: checkpoints always retain the canonical transcript")
	}
	if len(continuation.HandleKeys) == 0 {
		return fmt.Errorf("continuation.handle_keys must not be empty")
	}
	primary := 0
	seen := make(map[string]struct{}, len(continuation.HandleKeys))
	for index, key := range continuation.HandleKeys {
		path := fmt.Sprintf("continuation.handle_keys[%d]", index)
		if err := validateIdentifier(key.ID, path+".id"); err != nil {
			return err
		}
		if _, exists := seen[key.ID]; exists {
			return fmt.Errorf("%s duplicate key ID %q", path, key.ID)
		}
		seen[key.ID] = struct{}{}
		if key.Primary {
			primary++
		}
		if err := key.Secret.Validate(path + ".secret"); err != nil {
			return err
		}
	}
	if primary != 1 {
		return fmt.Errorf("continuation.handle_keys must contain exactly one primary key")
	}
	return nil
}

func (telemetry TelemetryConfig) validate(environment string) error {
	if telemetry.Logs.Format != "json" && telemetry.Logs.Format != "text" {
		return fmt.Errorf("telemetry.logs.format must be json or text")
	}
	switch telemetry.Logs.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("telemetry.logs.level is unsupported")
	}
	switch telemetry.ContentLogging {
	case "disabled", "redacted":
	default:
		return fmt.Errorf("telemetry.content_logging must be disabled or redacted")
	}
	if IsProductionEnvironment(environment) && telemetry.ContentLogging != "disabled" {
		return fmt.Errorf("telemetry.content_logging must be disabled in production")
	}
	if telemetry.Tracing.Enabled {
		if telemetry.Tracing.OTLPEndpoint == "" {
			return fmt.Errorf("telemetry.tracing.otlp_endpoint is required when tracing is enabled")
		}
		// Parse exactly as the tracer does (strconv.ParseFloat), so a value such
		// as "1/20" that big.Rat accepts cannot pass validation and then fail
		// tracer construction at startup.
		ratio, err := strconv.ParseFloat(telemetry.Tracing.SampleRatio, 64)
		if err != nil || math.IsNaN(ratio) || ratio < 0 || ratio > 1 {
			return fmt.Errorf("telemetry.tracing.sample_ratio must be a decimal between 0 and 1")
		}
	}
	return nil
}

func validateAddress(value, path string) error {
	if value == "" {
		return fmt.Errorf("%s must be non-empty host:port", path)
	}
	_, port, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("%s must be host:port: %w", path, err)
	}
	if number, err := strconv.Atoi(port); err != nil || number < 0 || number > 65535 {
		return fmt.Errorf("%s port must be between 0 and 65535", path)
	}
	return nil
}

// Validate rejects incomplete version identities rather than silently polling
// the unversioned queue when a deployment controller is misconfigured.
func (versioning WorkerVersioningConfig) Validate() error {
	if !versioning.Enabled {
		if versioning.DeploymentName != "" || versioning.BuildID != "" {
			return fmt.Errorf("temporal.worker.versioning identities require enabled: true")
		}
		return nil
	}
	for name, value := range map[string]string{"deployment_name": versioning.DeploymentName, "build_id": versioning.BuildID} {
		if err := validateIdentifier(value, "temporal.worker.versioning."+name); err != nil {
			return err
		}
		if strings.ContainsAny(value, "\x00") {
			return fmt.Errorf("temporal.worker.versioning.%s contains a control character", name)
		}
	}
	return nil
}
