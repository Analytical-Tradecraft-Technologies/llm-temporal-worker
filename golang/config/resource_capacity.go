package config

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
)

const resourceCapacityManifestSchemaVersion = "competition_resource_capacity_manifest/v1"
const productionMaxForecastEnsembleSize = 3

var resourceCapacitySHA256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type ProviderInflightLimit struct {
	RouteID              string `yaml:"route_id" json:"route_id"`
	MaxInflight          int    `yaml:"max_inflight" json:"max_inflight"`
	MaxRequestsPerWindow int    `yaml:"max_requests_per_window" json:"max_requests_per_window"`
	WindowSeconds        int    `yaml:"window_seconds" json:"window_seconds"`
}

type ResourceCapacityLimits struct {
	LLMGlobalMaxInflight                       int                     `yaml:"llm_global_max_inflight" json:"llm_global_max_inflight"`
	LLMGlobalMaxRequestsPerWindow              int                     `yaml:"llm_global_max_requests_per_window" json:"llm_global_max_requests_per_window"`
	LLMGlobalWindowSeconds                     int                     `yaml:"llm_global_window_seconds" json:"llm_global_window_seconds"`
	ProviderMaxInflight                        []ProviderInflightLimit `yaml:"provider_max_inflight" json:"provider_max_inflight"`
	SearchFetchMaxInflight                     int                     `yaml:"search_fetch_max_inflight" json:"search_fetch_max_inflight"`
	PythonStage2MaxInflight                    int                     `yaml:"python_stage2_max_inflight" json:"python_stage2_max_inflight"`
	ForecastEventMaxInflight                   int                     `yaml:"forecast_event_max_inflight" json:"forecast_event_max_inflight"`
	ForecastEventAdmissionWaitSeconds          int                     `yaml:"forecast_event_admission_wait_seconds" json:"forecast_event_admission_wait_seconds"`
	ProviderCallExecutionOverheadSeconds       int                     `yaml:"provider_call_execution_overhead_seconds" json:"provider_call_execution_overhead_seconds"`
	ProviderCallQueueSeconds                   int                     `yaml:"provider_call_queue_seconds" json:"provider_call_queue_seconds"`
	ProviderCallMaximumAttempts                int                     `yaml:"provider_call_maximum_attempts" json:"provider_call_maximum_attempts"`
	StageTwoExecutionOverheadSeconds           int                     `yaml:"stage_two_execution_overhead_seconds" json:"stage_two_execution_overhead_seconds"`
	StageTwoQueueSeconds                       int                     `yaml:"stage_two_queue_seconds" json:"stage_two_queue_seconds"`
	StageTwoSemaphoreWaitSeconds               int                     `yaml:"stage_two_semaphore_wait_seconds" json:"stage_two_semaphore_wait_seconds"`
	StageTwoMaximumAttempts                    int                     `yaml:"stage_two_maximum_attempts" json:"stage_two_maximum_attempts"`
	CloseExecutionOverheadSeconds              int                     `yaml:"close_execution_overhead_seconds" json:"close_execution_overhead_seconds"`
	CloseQueueSeconds                          int                     `yaml:"close_queue_seconds" json:"close_queue_seconds"`
	CloseMaximumAttempts                       int                     `yaml:"close_maximum_attempts" json:"close_maximum_attempts"`
	EvidenceCollectionExecutionOverheadSeconds int                     `yaml:"evidence_collection_execution_overhead_seconds" json:"evidence_collection_execution_overhead_seconds"`
	EvidenceCollectionQueueSeconds             int                     `yaml:"evidence_collection_queue_seconds" json:"evidence_collection_queue_seconds"`
	EvidenceCollectionMaximumAttempts          int                     `yaml:"evidence_collection_maximum_attempts" json:"evidence_collection_maximum_attempts"`
	PanelPrepareMaxInflight                    int                     `yaml:"panel_prepare_max_inflight" json:"panel_prepare_max_inflight"`
	PanelLLMMaxInflight                        int                     `yaml:"panel_llm_max_inflight" json:"panel_llm_max_inflight"`
	InferenceArtifactizeExecutionSeconds       int                     `yaml:"inference_artifactize_execution_seconds" json:"inference_artifactize_execution_seconds"`
	InferenceArtifactizeQueueSeconds           int                     `yaml:"inference_artifactize_queue_seconds" json:"inference_artifactize_queue_seconds"`
	InferenceArtifactizeMaximumAttempts        int                     `yaml:"inference_artifactize_maximum_attempts" json:"inference_artifactize_maximum_attempts"`
	PrepareAssessmentExecutionSeconds          int                     `yaml:"prepare_assessment_execution_seconds" json:"prepare_assessment_execution_seconds"`
	PrepareAssessmentQueueSeconds              int                     `yaml:"prepare_assessment_queue_seconds" json:"prepare_assessment_queue_seconds"`
	PrepareAssessmentMaximumAttempts           int                     `yaml:"prepare_assessment_maximum_attempts" json:"prepare_assessment_maximum_attempts"`
	BuildAnalysisPanelExecutionSeconds         int                     `yaml:"build_analysis_panel_execution_seconds" json:"build_analysis_panel_execution_seconds"`
	BuildAnalysisPanelQueueSeconds             int                     `yaml:"build_analysis_panel_queue_seconds" json:"build_analysis_panel_queue_seconds"`
	BuildAnalysisPanelMaximumAttempts          int                     `yaml:"build_analysis_panel_maximum_attempts" json:"build_analysis_panel_maximum_attempts"`
	CheckpointAnalysisFrontierExecutionSeconds int                     `yaml:"checkpoint_analysis_frontier_execution_seconds" json:"checkpoint_analysis_frontier_execution_seconds"`
	CheckpointAnalysisFrontierQueueSeconds     int                     `yaml:"checkpoint_analysis_frontier_queue_seconds" json:"checkpoint_analysis_frontier_queue_seconds"`
	CheckpointAnalysisFrontierMaximumAttempts  int                     `yaml:"checkpoint_analysis_frontier_maximum_attempts" json:"checkpoint_analysis_frontier_maximum_attempts"`
	LoadAnalysisFrontierExecutionSeconds       int                     `yaml:"load_analysis_frontier_execution_seconds" json:"load_analysis_frontier_execution_seconds"`
	LoadAnalysisFrontierQueueSeconds           int                     `yaml:"load_analysis_frontier_queue_seconds" json:"load_analysis_frontier_queue_seconds"`
	LoadAnalysisFrontierMaximumAttempts        int                     `yaml:"load_analysis_frontier_maximum_attempts" json:"load_analysis_frontier_maximum_attempts"`
	CallerOwnedEvidenceIngestExecutionSeconds  int                     `yaml:"caller_owned_evidence_ingest_execution_seconds" json:"caller_owned_evidence_ingest_execution_seconds"`
	CallerOwnedEvidenceIngestQueueSeconds      int                     `yaml:"caller_owned_evidence_ingest_queue_seconds" json:"caller_owned_evidence_ingest_queue_seconds"`
	CallerOwnedEvidenceIngestMaximumAttempts   int                     `yaml:"caller_owned_evidence_ingest_maximum_attempts" json:"caller_owned_evidence_ingest_maximum_attempts"`
	AssembleEvidenceAssessmentExecutionSeconds int                     `yaml:"assemble_evidence_assessment_execution_seconds" json:"assemble_evidence_assessment_execution_seconds"`
	AssembleEvidenceAssessmentQueueSeconds     int                     `yaml:"assemble_evidence_assessment_queue_seconds" json:"assemble_evidence_assessment_queue_seconds"`
	AssembleEvidenceAssessmentMaximumAttempts  int                     `yaml:"assemble_evidence_assessment_maximum_attempts" json:"assemble_evidence_assessment_maximum_attempts"`
	LoadAnalyticContextExecutionSeconds        int                     `yaml:"load_analytic_context_execution_seconds" json:"load_analytic_context_execution_seconds"`
	LoadAnalyticContextQueueSeconds            int                     `yaml:"load_analytic_context_queue_seconds" json:"load_analytic_context_queue_seconds"`
	LoadAnalyticContextMaximumAttempts         int                     `yaml:"load_analytic_context_maximum_attempts" json:"load_analytic_context_maximum_attempts"`
	PanelPrepareActivityMaxBytes               int64                   `yaml:"panel_prepare_activity_max_bytes" json:"panel_prepare_activity_max_bytes"`
}

func (limits ResourceCapacityLimits) validate() error {
	if limits.LLMGlobalMaxInflight < 64 || limits.LLMGlobalMaxInflight > 4096 || len(limits.ProviderMaxInflight) < 1 || len(limits.ProviderMaxInflight) > 64 {
		return errors.New("resource_capacity LLM inflight limits are invalid")
	}
	if limits.LLMGlobalMaxRequestsPerWindow < 1 || limits.LLMGlobalMaxRequestsPerWindow > 1_000_000 || limits.LLMGlobalWindowSeconds < 1 || limits.LLMGlobalWindowSeconds > 3600 {
		return errors.New("resource_capacity global LLM request window is invalid")
	}
	previous, total := "", 0
	for index, limit := range limits.ProviderMaxInflight {
		if err := validateIdentifier(limit.RouteID, "resource_capacity.limits.provider_max_inflight.route_id"); err != nil {
			return err
		}
		if index > 0 && limit.RouteID <= previous {
			return errors.New("resource_capacity provider routes must be unique and sorted")
		}
		if limit.MaxInflight < 1 || limit.MaxInflight > limits.LLMGlobalMaxInflight {
			return errors.New("resource_capacity provider inflight limit is invalid")
		}
		if limit.MaxRequestsPerWindow < 1 || limit.MaxRequestsPerWindow > limits.LLMGlobalMaxRequestsPerWindow || limit.WindowSeconds < 1 || limit.WindowSeconds > 3600 {
			return errors.New("resource_capacity provider request window is invalid")
		}
		total += limit.MaxInflight
		previous = limit.RouteID
	}
	if total < limits.LLMGlobalMaxInflight {
		return errors.New("resource_capacity provider limits cannot satisfy the global LLM limit")
	}
	if limits.SearchFetchMaxInflight < 64 || limits.SearchFetchMaxInflight > 4096 || limits.PythonStage2MaxInflight < 6 || limits.PythonStage2MaxInflight > 4096 {
		return errors.New("resource_capacity search/fetch or Python Stage2 limit is invalid")
	}
	if limits.ForecastEventMaxInflight != 2 || limits.ForecastEventAdmissionWaitSeconds != 5 {
		return errors.New("resource_capacity forecast event admission policy is invalid")
	}
	if limits.ForecastEventMaxInflight*productionMaxForecastEnsembleSize > limits.PythonStage2MaxInflight {
		return errors.New("resource_capacity Python Stage2 slots cannot admit every forecast event ensemble")
	}
	if limits.PanelPrepareMaxInflight != 8 || limits.PanelLLMMaxInflight != 8 || limits.PanelLLMMaxInflight > limits.LLMGlobalMaxInflight {
		return errors.New("resource_capacity panel concurrency is invalid")
	}
	if limits.PanelPrepareActivityMaxBytes != 1_310_720 {
		return errors.New("resource_capacity panel prepare Activity byte bound is invalid")
	}
	for _, limit := range limits.ProviderMaxInflight {
		if limit.MaxInflight < limits.PanelLLMMaxInflight {
			return fmt.Errorf("resource_capacity provider route %q cannot admit one panel wave", limit.RouteID)
		}
	}
	for label, value := range map[string]int{
		"provider_call_execution_overhead_seconds":       limits.ProviderCallExecutionOverheadSeconds,
		"stage_two_execution_overhead_seconds":           limits.StageTwoExecutionOverheadSeconds,
		"close_execution_overhead_seconds":               limits.CloseExecutionOverheadSeconds,
		"evidence_collection_execution_overhead_seconds": limits.EvidenceCollectionExecutionOverheadSeconds,
		"inference_artifactize_execution_seconds":        limits.InferenceArtifactizeExecutionSeconds,
		"prepare_assessment_execution_seconds":           limits.PrepareAssessmentExecutionSeconds,
		"build_analysis_panel_execution_seconds":         limits.BuildAnalysisPanelExecutionSeconds,
		"assemble_evidence_assessment_execution_seconds": limits.AssembleEvidenceAssessmentExecutionSeconds,
		"load_analytic_context_execution_seconds":        limits.LoadAnalyticContextExecutionSeconds,
		"checkpoint_analysis_frontier_execution_seconds": limits.CheckpointAnalysisFrontierExecutionSeconds,
		"load_analysis_frontier_execution_seconds":       limits.LoadAnalysisFrontierExecutionSeconds,
		"caller_owned_evidence_ingest_execution_seconds": limits.CallerOwnedEvidenceIngestExecutionSeconds,
	} {
		if value < 1 || value > 300 {
			return fmt.Errorf("resource_capacity.limits.%s is invalid", label)
		}
	}
	if limits.StageTwoQueueSeconds != 30 || limits.StageTwoSemaphoreWaitSeconds != 30 {
		return errors.New("resource_capacity Stage Two Activity queue or semaphore wait bound is invalid")
	}
	for label, value := range map[string]int{
		"provider_call_queue_seconds":                limits.ProviderCallQueueSeconds,
		"close_queue_seconds":                        limits.CloseQueueSeconds,
		"evidence_collection_queue_seconds":          limits.EvidenceCollectionQueueSeconds,
		"inference_artifactize_queue_seconds":        limits.InferenceArtifactizeQueueSeconds,
		"prepare_assessment_queue_seconds":           limits.PrepareAssessmentQueueSeconds,
		"build_analysis_panel_queue_seconds":         limits.BuildAnalysisPanelQueueSeconds,
		"assemble_evidence_assessment_queue_seconds": limits.AssembleEvidenceAssessmentQueueSeconds,
		"load_analytic_context_queue_seconds":        limits.LoadAnalyticContextQueueSeconds,
		"checkpoint_analysis_frontier_queue_seconds": limits.CheckpointAnalysisFrontierQueueSeconds,
		"load_analysis_frontier_queue_seconds":       limits.LoadAnalysisFrontierQueueSeconds,
		"caller_owned_evidence_ingest_queue_seconds": limits.CallerOwnedEvidenceIngestQueueSeconds,
	} {
		if value < 1 || value > 30 {
			return fmt.Errorf("resource_capacity.limits.%s is invalid", label)
		}
	}
	if limits.ProviderCallMaximumAttempts != 1 || limits.StageTwoMaximumAttempts != 2 ||
		limits.CloseMaximumAttempts != 2 || limits.EvidenceCollectionMaximumAttempts != 1 ||
		limits.InferenceArtifactizeMaximumAttempts != 2 || limits.PrepareAssessmentMaximumAttempts != 2 ||
		limits.BuildAnalysisPanelMaximumAttempts != 2 || limits.AssembleEvidenceAssessmentMaximumAttempts != 2 ||
		limits.LoadAnalyticContextMaximumAttempts != 2 || limits.CheckpointAnalysisFrontierMaximumAttempts != 2 ||
		limits.LoadAnalysisFrontierMaximumAttempts != 2 || limits.CallerOwnedEvidenceIngestMaximumAttempts != 2 {
		return errors.New("resource_capacity Activity attempt policy is invalid")
	}
	return nil
}

type ResourceCapacityConfig struct {
	ManifestFile    string                 `yaml:"manifest_file" json:"manifest_file"`
	TrustRootFile   string                 `yaml:"trust_root_file" json:"trust_root_file"`
	TrustRootSHA256 string                 `yaml:"trust_root_sha256" json:"trust_root_sha256"`
	ManifestSHA256  string                 `yaml:"manifest_sha256" json:"manifest_sha256"`
	ArtifactID      string                 `yaml:"artifact_id" json:"artifact_id"`
	ArtifactLocator string                 `yaml:"artifact_locator" json:"artifact_locator"`
	GenerationID    string                 `yaml:"generation_id" json:"generation_id"`
	Limits          ResourceCapacityLimits `yaml:"limits" json:"limits"`
}

func (value ResourceCapacityConfig) configured() bool {
	return value.ManifestFile != "" || value.TrustRootFile != "" || value.ManifestSHA256 != "" || value.GenerationID != ""
}

func (value ResourceCapacityConfig) validate(environment string) error {
	if !value.configured() {
		if environment == "production" {
			return errors.New("resource_capacity is required in production")
		}
		return nil
	}
	for label, path := range map[string]string{"manifest_file": value.ManifestFile, "trust_root_file": value.TrustRootFile} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("resource_capacity.%s must be an absolute canonical path", label)
		}
	}
	for label, digest := range map[string]string{"trust_root_sha256": value.TrustRootSHA256, "manifest_sha256": value.ManifestSHA256} {
		if !resourceCapacitySHA256Pattern.MatchString(digest) {
			return fmt.Errorf("resource_capacity.%s is invalid", label)
		}
	}
	if err := validateIdentifier(value.ArtifactID, "resource_capacity.artifact_id"); err != nil {
		return err
	}
	locator, err := url.Parse(value.ArtifactLocator)
	if err != nil || locator.Scheme != "s3" || locator.Host == "" || locator.Path == "" {
		return errors.New("resource_capacity.artifact_locator must be an explicit s3 URI")
	}
	if err := validateIdentifier(value.GenerationID, "resource_capacity.generation_id"); err != nil {
		return err
	}
	return value.Limits.validate()
}

type resourceCapacityManifest struct {
	SchemaVersion string                 `json:"schema_version"`
	GenerationID  string                 `json:"generation_id"`
	Limits        ResourceCapacityLimits `json:"limits"`
}

type signedResourceCapacityManifest struct {
	SchemaVersion string                   `json:"schema_version"`
	KeyID         string                   `json:"key_id"`
	Manifest      resourceCapacityManifest `json:"manifest"`
	Signature     string                   `json:"signature"`
}

func (value ResourceCapacityConfig) validateRoutes(models map[string]ModelConfig) error {
	if !value.configured() {
		return nil
	}
	configuredRoutes := make(map[string]struct{})
	for _, modelName := range sortedKeys(models) {
		for _, route := range models[modelName].Routes {
			configuredRoutes[route.ID] = struct{}{}
		}
	}
	if len(configuredRoutes) != len(value.Limits.ProviderMaxInflight) {
		return errors.New("resource_capacity provider limits must exactly cover configured inference routes")
	}
	for _, limit := range value.Limits.ProviderMaxInflight {
		if _, exists := configuredRoutes[limit.RouteID]; !exists {
			return fmt.Errorf("resource_capacity provider route %q is not configured", limit.RouteID)
		}
	}
	return nil
}

type resourceCapacitySigningPayload struct {
	SchemaVersion string                   `json:"schema_version"`
	KeyID         string                   `json:"key_id"`
	Manifest      resourceCapacityManifest `json:"manifest"`
}

type capacityTrustKey struct {
	KeyID     string `json:"key_id"`
	PublicKey string `json:"public_key"`
}

type capacityTrustRoot struct {
	SchemaVersion string             `json:"schema_version"`
	Keys          []capacityTrustKey `json:"keys"`
}

type VerifiedResourceCapacity struct {
	ManifestSHA256  string
	ArtifactID      string
	ArtifactLocator string
	GenerationID    string
	SignatureKeyID  string
	Signature       string
	Limits          ResourceCapacityLimits
}

func decodeCanonicalCapacity(encoded []byte, maximum int, destination any, label string) error {
	if len(encoded) < 1 || len(encoded) > maximum {
		return fmt.Errorf("%s is outside the size bound", label)
	}
	if err := json.Unmarshal(encoded, destination); err != nil {
		return fmt.Errorf("decode %s: %w", label, err)
	}
	canonical, err := json.Marshal(destination)
	if err != nil || !bytes.Equal(encoded, canonical) {
		return fmt.Errorf("%s is not strict canonical JSON", label)
	}
	return nil
}

// VerifyResourceCapacity independently authenticates the operator-owned
// manifest before any runtime or provider adapter can consume its limits.
func VerifyResourceCapacity(configured ResourceCapacityConfig) (VerifiedResourceCapacity, error) {
	if err := configured.validate("production"); err != nil {
		return VerifiedResourceCapacity{}, err
	}
	manifestBytes, err := os.ReadFile(configured.ManifestFile)
	if err != nil {
		return VerifiedResourceCapacity{}, fmt.Errorf("read resource capacity manifest: %w", err)
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	if hex.EncodeToString(manifestDigest[:]) != configured.ManifestSHA256 {
		return VerifiedResourceCapacity{}, errors.New("resource capacity manifest does not match configured sha256")
	}
	var signed signedResourceCapacityManifest
	if err := decodeCanonicalCapacity(manifestBytes, 32<<10, &signed, "resource capacity manifest"); err != nil {
		return VerifiedResourceCapacity{}, err
	}
	if signed.SchemaVersion != resourceCapacityManifestSchemaVersion || signed.Manifest.SchemaVersion != resourceCapacityManifestSchemaVersion || signed.Manifest.GenerationID != configured.GenerationID || !reflect.DeepEqual(signed.Manifest.Limits, configured.Limits) {
		return VerifiedResourceCapacity{}, errors.New("resource capacity manifest does not exact-match configured immutable capacity")
	}
	if err := signed.Manifest.Limits.validate(); err != nil {
		return VerifiedResourceCapacity{}, err
	}
	trustRootBytes, err := os.ReadFile(configured.TrustRootFile)
	if err != nil {
		return VerifiedResourceCapacity{}, fmt.Errorf("read resource capacity trust root: %w", err)
	}
	rootDigest := sha256.Sum256(trustRootBytes)
	if hex.EncodeToString(rootDigest[:]) != configured.TrustRootSHA256 {
		return VerifiedResourceCapacity{}, errors.New("resource capacity trust root does not match configured sha256")
	}
	var root capacityTrustRoot
	if err := decodeCanonicalCapacity(trustRootBytes, 16<<10, &root, "resource capacity trust root"); err != nil {
		return VerifiedResourceCapacity{}, err
	}
	if root.SchemaVersion != "competition_worker_release_trust_root/v1" || len(root.Keys) < 1 || len(root.Keys) > 32 {
		return VerifiedResourceCapacity{}, errors.New("resource capacity trust root is invalid")
	}
	seen := make(map[string]struct{}, len(root.Keys))
	var publicKey ed25519.PublicKey
	for _, key := range root.Keys {
		if err := validateIdentifier(key.KeyID, "resource capacity trust key_id"); err != nil {
			return VerifiedResourceCapacity{}, err
		}
		if _, exists := seen[key.KeyID]; exists {
			return VerifiedResourceCapacity{}, errors.New("resource capacity trust root repeats a key_id")
		}
		seen[key.KeyID] = struct{}{}
		decoded, decodeErr := base64.StdEncoding.Strict().DecodeString(key.PublicKey)
		if decodeErr != nil || len(decoded) != ed25519.PublicKeySize {
			return VerifiedResourceCapacity{}, errors.New("resource capacity trust root contains an invalid Ed25519 key")
		}
		if key.KeyID == signed.KeyID {
			publicKey = append(ed25519.PublicKey(nil), decoded...)
		}
	}
	if publicKey == nil {
		return VerifiedResourceCapacity{}, errors.New("resource capacity signing key is not trusted")
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(signed.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return VerifiedResourceCapacity{}, errors.New("resource capacity signature is invalid")
	}
	payload, err := json.Marshal(resourceCapacitySigningPayload{SchemaVersion: resourceCapacityManifestSchemaVersion, KeyID: signed.KeyID, Manifest: signed.Manifest})
	if err != nil || !ed25519.Verify(publicKey, payload, signature) {
		return VerifiedResourceCapacity{}, errors.New("resource capacity signature verification failed")
	}
	limits := signed.Manifest.Limits
	limits.ProviderMaxInflight = append([]ProviderInflightLimit(nil), limits.ProviderMaxInflight...)
	return VerifiedResourceCapacity{ManifestSHA256: configured.ManifestSHA256, ArtifactID: configured.ArtifactID, ArtifactLocator: configured.ArtifactLocator, GenerationID: configured.GenerationID, SignatureKeyID: signed.KeyID, Signature: signed.Signature, Limits: limits}, nil
}
