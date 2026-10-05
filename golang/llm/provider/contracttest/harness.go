// Package contracttest validates governed provider adapter fixtures.
//
// It deliberately operates only on checked-in fixture files. The validator
// never records, rewrites, or downloads fixture data while a test is running.
package contracttest

import (
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v4"
)

const manifestName = "manifest.yaml"

var (
	privateKeyPattern      = regexp.MustCompile(`-----BEGIN(?: [A-Z]+)? PRIVATE KEY-----`)
	awsAccessKeyPattern    = regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`)
	githubTokenPattern     = regexp.MustCompile(`\b(?:gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,})\b`)
	slackTokenPattern      = regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{10,}\b`)
	credentialFieldPattern = regexp.MustCompile(`(?i)(?:authorization|api[_-]?key|access[_-]?token|secret[_-]?key)\s*(?:\\?["']\s*)?[:=]\s*(?:\\?["']\s*)?(?:bearer\s+)?[A-Za-z0-9_./=+-]{8,}`)
	profileIDPattern       = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	publicServiceClasses   = []string{"economy", "standard", "priority"}
)

// Coverage identifies whether a profile is only structurally governed or has
// complete, capability-aware fixture coverage.
type Coverage string

const (
	CoverageBootstrap Coverage = "bootstrap"
	CoverageEnforced  Coverage = "enforced"
)

// Profile is a validated fixture profile included in a repository report.
type Profile struct {
	ID       string
	Coverage Coverage
	Path     string
}

// Report separates bootstrap and enforced profiles so a green structural gate
// cannot be mistaken for full matrix coverage.
type Report struct {
	Bootstrap []Profile
	Enforced  []Profile
}

// RequireAllEnforced turns a structural report into the release gate used
// after every production profile has completed its dedicated fixture task.
func (report Report) RequireAllEnforced() error {
	if len(report.Bootstrap) == 0 {
		return nil
	}
	profiles := make([]string, 0, len(report.Bootstrap))
	for _, profile := range report.Bootstrap {
		profiles = append(profiles, profile.ID)
	}
	sort.Strings(profiles)
	return fmt.Errorf("adapter contract profiles remain bootstrap: %s", strings.Join(profiles, ", "))
}

// Manifest is the checked-in schema for one provider fixture profile.
type Manifest struct {
	Version        int                       `yaml:"version"`
	ID             string                    `yaml:"id"`
	Provider       string                    `yaml:"provider"`
	Family         string                    `yaml:"family"`
	Coverage       Coverage                  `yaml:"coverage"`
	Metadata       string                    `yaml:"metadata"`
	ServiceClasses map[string]map[string]any `yaml:"service_classes"`
	Cases          []Case                    `yaml:"cases"`
}

// Case records the fixture artifacts that demonstrate one code-owned contract
// case. Bootstrap profiles may declare a subset; enforced profiles must meet
// the registry in registry.go.
type Case struct {
	ID        string    `yaml:"id"`
	Artifacts Artifacts `yaml:"artifacts"`
}

// Artifacts are the semantic, wire, and event fixtures associated with a case.
// An artifact is optional when that case has no representation of that kind,
// but every declared artifact must be a safe regular file in its profile.
type Artifacts struct {
	Semantic string `yaml:"semantic"`
	Wire     string `yaml:"wire"`
	Events   string `yaml:"events"`
}

// Metadata is kept in a separate file so fixture provenance can be reviewed
// independently of the case matrix.
type Metadata struct {
	Profile                  string            `yaml:"profile"`
	UpstreamURL              string            `yaml:"upstream_url"`
	UpstreamDate             string            `yaml:"upstream_date"`
	SDKVersion               string            `yaml:"sdk_version"`
	Provenance               string            `yaml:"provenance"`
	Redactions               []string          `yaml:"redactions"`
	CapabilityFacts          map[string]string `yaml:"capability_facts"`
	GeneratedFieldExemptions []string          `yaml:"generated_field_exemptions"`
}

// ValidateRepository finds every provider fixture manifest below root and
// validates its schema, metadata, declared artifacts, and raw fixture bytes.
func ValidateRepository(root string) (Report, error) {
	root = filepath.Clean(root)
	providerRoot := filepath.Join(root, "llm", "provider")
	manifests, err := findManifests(providerRoot)
	if err != nil {
		return Report{}, err
	}
	if len(manifests) == 0 {
		return Report{}, fmt.Errorf("adapter contract repository has no manifests")
	}

	report := Report{}
	contractRoots := make(map[string]struct{})
	profileIDs := make(map[string]struct{}, len(manifests))
	for _, manifestPath := range manifests {
		profile, contractRoot, err := validateManifest(root, manifestPath)
		if err != nil {
			return Report{}, err
		}
		if _, exists := profileIDs[profile.ID]; exists {
			return Report{}, fmt.Errorf("adapter contract repository has duplicate profile IDs")
		}
		profileIDs[profile.ID] = struct{}{}
		contractRoots[contractRoot] = struct{}{}
		switch profile.Coverage {
		case CoverageBootstrap:
			report.Bootstrap = append(report.Bootstrap, profile)
		case CoverageEnforced:
			report.Enforced = append(report.Enforced, profile)
		}
	}
	if err := scanContractRoots(root, contractRoots); err != nil {
		return Report{}, err
	}
	sortProfiles(report.Bootstrap)
	sortProfiles(report.Enforced)
	return report, nil
}

func findManifests(providerRoot string) ([]string, error) {
	var manifests []string
	err := filepath.WalkDir(providerRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && entry.Name() == manifestName {
			if entry.Type()&fs.ModeSymlink != 0 {
				return fmt.Errorf("adapter contract manifest must not be a symlink")
			}
			manifests = append(manifests, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("adapter contract manifest discovery failed")
	}
	sort.Strings(manifests)
	return manifests, nil
}

func validateManifest(root, manifestPath string) (Profile, string, error) {
	manifest, err := loadManifest(manifestPath)
	if err != nil {
		return Profile{}, "", err
	}
	profileDir := filepath.Dir(manifestPath)
	relativeDir, err := filepath.Rel(root, profileDir)
	if err != nil || strings.HasPrefix(relativeDir, "..") {
		return Profile{}, "", fmt.Errorf("adapter contract manifest path is outside repository")
	}
	contractRoot, err := contractRootForManifest(manifestPath)
	if err != nil {
		return Profile{}, "", fmt.Errorf("adapter contract manifest is not below testdata/contracts")
	}
	if err := validateManifestShape(manifest, profileDir); err != nil {
		return Profile{}, "", fmt.Errorf("adapter contract %s: %w", filepath.ToSlash(relativeDir), err)
	}
	metadataPath, err := resolveArtifact(profileDir, manifest.Metadata)
	if err != nil {
		return Profile{}, "", fmt.Errorf("adapter contract %s: invalid metadata path", filepath.ToSlash(relativeDir))
	}
	if err := requireRegularFixture(metadataPath); err != nil {
		return Profile{}, "", fmt.Errorf("adapter contract %s: invalid metadata path", filepath.ToSlash(relativeDir))
	}
	metadata, err := loadMetadata(metadataPath)
	if err != nil {
		return Profile{}, "", fmt.Errorf("adapter contract %s: invalid metadata", filepath.ToSlash(relativeDir))
	}
	if err := validateMetadata(metadata, manifest.ID); err != nil {
		return Profile{}, "", fmt.Errorf("adapter contract %s: %w", filepath.ToSlash(relativeDir), err)
	}
	if err := validateArtifacts(profileDir, manifest.Cases); err != nil {
		return Profile{}, "", fmt.Errorf("adapter contract %s: %w", filepath.ToSlash(relativeDir), err)
	}
	if manifest.Coverage == CoverageEnforced {
		if err := validateEnforcedCoverage(manifest, metadata); err != nil {
			return Profile{}, "", fmt.Errorf("adapter contract %s: %w", filepath.ToSlash(relativeDir), err)
		}
	}
	return Profile{ID: manifest.ID, Coverage: manifest.Coverage, Path: filepath.ToSlash(relativeDir)}, contractRoot, nil
}

func loadManifest(path string) (Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("manifest cannot be read")
	}
	var manifest Manifest
	if err := yaml.Load(data, &manifest, yaml.WithKnownFields(), yaml.WithUniqueKeys()); err != nil {
		return Manifest{}, fmt.Errorf("manifest is not valid YAML")
	}
	return manifest, nil
}

func loadMetadata(path string) (Metadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Metadata{}, err
	}
	var metadata Metadata
	if err := yaml.Load(data, &metadata, yaml.WithKnownFields(), yaml.WithUniqueKeys()); err != nil {
		return Metadata{}, err
	}
	return metadata, nil
}

func validateManifestShape(manifest Manifest, profileDir string) error {
	if manifest.Version != 1 {
		return fmt.Errorf("manifest version must be 1")
	}
	if manifest.ID == "" || manifest.Provider == "" || manifest.Family == "" {
		return fmt.Errorf("manifest id, provider, and family are required")
	}
	if !profileIDPattern.MatchString(manifest.ID) {
		return fmt.Errorf("manifest id must be a lowercase hyphenated identifier")
	}
	if manifest.Coverage != CoverageBootstrap && manifest.Coverage != CoverageEnforced {
		return fmt.Errorf("manifest coverage must be bootstrap or enforced")
	}
	if manifest.Metadata == "" {
		return fmt.Errorf("manifest metadata is required")
	}
	if err := validateServiceClasses(manifest.ServiceClasses); err != nil {
		return err
	}
	if len(manifest.Cases) == 0 {
		return fmt.Errorf("manifest must declare at least one case")
	}
	seen := make(map[string]struct{}, len(manifest.Cases))
	for _, fixtureCase := range manifest.Cases {
		if fixtureCase.ID == "" {
			return fmt.Errorf("manifest case id is required")
		}
		if _, ok := seen[fixtureCase.ID]; ok {
			return fmt.Errorf("manifest case ids must be unique")
		}
		seen[fixtureCase.ID] = struct{}{}
		requirement, ok := registeredCase(fixtureCase.ID)
		if !ok {
			return fmt.Errorf("manifest references an unregistered case")
		}
		if fixtureCase.Artifacts.Semantic == "" && fixtureCase.Artifacts.Wire == "" && fixtureCase.Artifacts.Events == "" {
			return fmt.Errorf("manifest case has no artifacts")
		}
		for _, artifact := range requirement.Artifacts {
			if !fixtureCase.Artifacts.has(artifact) {
				return fmt.Errorf("manifest case lacks its documented %s artifact", artifact)
			}
		}
	}
	return nil
}

func validateServiceClasses(serviceClasses map[string]map[string]any) error {
	if _, exists := serviceClasses["provider_default"]; exists {
		return fmt.Errorf("manifest service_classes must not declare provider_default")
	}
	for _, serviceClass := range publicServiceClasses {
		facts, exists := serviceClasses[serviceClass]
		if !exists || len(facts) == 0 {
			return fmt.Errorf("manifest service_classes must declare non-empty %s facts", serviceClass)
		}
	}
	return nil
}

func validateMetadata(metadata Metadata, profileID string) error {
	if metadata.Profile != profileID {
		return fmt.Errorf("metadata profile does not match manifest")
	}
	parsedURL, err := url.ParseRequestURI(metadata.UpstreamURL)
	if err != nil || parsedURL.Scheme != "https" || parsedURL.Host == "" {
		return fmt.Errorf("metadata upstream_url must be an HTTPS URL")
	}
	if _, err := time.Parse(time.DateOnly, metadata.UpstreamDate); err != nil {
		return fmt.Errorf("metadata upstream_date must be YYYY-MM-DD")
	}
	if metadata.SDKVersion == "" {
		return fmt.Errorf("metadata sdk_version is required")
	}
	switch metadata.Provenance {
	case "captured", "synthetic", "mixed":
	default:
		return fmt.Errorf("metadata provenance is invalid")
	}
	if len(metadata.Redactions) == 0 {
		return fmt.Errorf("metadata redactions are required")
	}
	for _, redaction := range metadata.Redactions {
		if strings.TrimSpace(redaction) == "" {
			return fmt.Errorf("metadata redactions must be non-empty")
		}
	}
	if len(metadata.CapabilityFacts) == 0 {
		return fmt.Errorf("metadata capability_facts are required")
	}
	for capability, fact := range metadata.CapabilityFacts {
		if strings.TrimSpace(capability) == "" || strings.TrimSpace(fact) == "" {
			return fmt.Errorf("metadata capability_facts must be non-empty")
		}
	}
	if metadata.GeneratedFieldExemptions == nil {
		return fmt.Errorf("metadata generated_field_exemptions must be present")
	}
	return nil
}

func validateArtifacts(profileDir string, cases []Case) error {
	for _, fixtureCase := range cases {
		for _, artifact := range []string{fixtureCase.Artifacts.Semantic, fixtureCase.Artifacts.Wire, fixtureCase.Artifacts.Events} {
			if artifact == "" {
				continue
			}
			path, err := resolveArtifact(profileDir, artifact)
			if err != nil {
				return fmt.Errorf("case has an invalid artifact path")
			}
			if err := requireRegularFixture(path); err != nil {
				return fmt.Errorf("case references a missing fixture")
			}
		}
	}
	return nil
}

func requireRegularFixture(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("fixture is not a regular file")
	}
	return nil
}

func resolveArtifact(profileDir, name string) (string, error) {
	if name == "" || filepath.IsAbs(name) {
		return "", fmt.Errorf("fixture path is empty or absolute")
	}
	path := filepath.Clean(filepath.Join(profileDir, name))
	relative, err := filepath.Rel(profileDir, path)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("fixture path escapes profile")
	}
	return path, nil
}

func contractRootForManifest(manifestPath string) (string, error) {
	for directory := filepath.Dir(manifestPath); ; directory = filepath.Dir(directory) {
		if filepath.Base(directory) == "contracts" && filepath.Base(filepath.Dir(directory)) == "testdata" {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", fmt.Errorf("not a contract fixture path")
		}
	}
}

func scanContractRoots(root string, contractRoots map[string]struct{}) error {
	roots := make([]string, 0, len(contractRoots))
	for contractRoot := range contractRoots {
		roots = append(roots, contractRoot)
	}
	sort.Strings(roots)
	for _, contractRoot := range roots {
		if err := scanFixtureDirectory(root, contractRoot); err != nil {
			return err
		}
	}
	return nil
}

func scanFixtureDirectory(root, fixtureRoot string) error {
	return filepath.WalkDir(fixtureRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fixturePathError(root, path, "cannot be read")
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return fixturePathError(root, path, "must not be a symlink")
		}
		if !entry.Type().IsRegular() {
			return fixturePathError(root, path, "must be a regular file")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return fixturePathError(root, path, "cannot be read")
		}
		if containsUnsafeFixtureBytes(data) {
			return fixturePathError(root, path, "contains credential-like material")
		}
		return nil
	})
}

func fixturePathError(root, path, message string) error {
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("fixture %s", message)
	}
	return fmt.Errorf("fixture %q %s", filepath.ToSlash(relative), message)
}

func containsUnsafeFixtureBytes(data []byte) bool {
	if privateKeyPattern.Match(data) || awsAccessKeyPattern.Match(data) || githubTokenPattern.Match(data) || slackTokenPattern.Match(data) {
		return true
	}
	for _, match := range credentialFieldPattern.FindAll(data, -1) {
		if !containsRedactionMarker(match) {
			return true
		}
	}
	return false
}

func containsRedactionMarker(value []byte) bool {
	normalized := strings.ToLower(string(value))
	return strings.Contains(normalized, "redacted") || strings.Contains(normalized, "placeholder") || strings.Contains(normalized, "example")
}

func sortProfiles(profiles []Profile) {
	sort.Slice(profiles, func(left, right int) bool {
		return profiles[left].ID < profiles[right].ID
	})
}
