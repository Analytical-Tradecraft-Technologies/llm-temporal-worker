package config_test

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
)

// fall2026CapacityFixture is TML's resource-capacity-production.json
// (forecast-capacity-2026-09-26) signed by TML's own attestation command,
// research/ai_ach/cmd/resource-capacity-attestation on branch
// research/ach-p0-frontier-profile, with TML's capacity test key
// capacity-ci-v1 (Ed25519 seed "c" x 32). Ed25519 is deterministic, so the
// bytes are reproducible from that command.
const fall2026CapacityFixture = "testdata/resource-capacity/fall-2026-capacity-ci-v1.signed.json"

// signedFall2026Capacity returns a production resource_capacity block that
// references the TML-signed manifest and a trust root holding capacity-ci-v1.
func signedFall2026Capacity(t *testing.T) config.ResourceCapacityConfig {
	t.Helper()
	manifest, err := os.ReadFile(fall2026CapacityFixture)
	if err != nil {
		t.Fatal(err)
	}
	var signed struct {
		Manifest struct {
			GenerationID string                        `json:"generation_id"`
			Limits       config.ResourceCapacityLimits `json:"limits"`
		} `json:"manifest"`
	}
	if err := json.Unmarshal(manifest, &signed); err != nil {
		t.Fatal(err)
	}
	publicKey := ed25519.NewKeyFromSeed([]byte(strings.Repeat("c", ed25519.SeedSize))).Public().(ed25519.PublicKey)
	type trustKey struct {
		KeyID     string `json:"key_id"`
		PublicKey string `json:"public_key"`
	}
	// Field order matches the worker's canonical trust-root encoding.
	root, err := json.Marshal(struct {
		SchemaVersion string     `json:"schema_version"`
		Keys          []trustKey `json:"keys"`
	}{"competition_worker_release_trust_root/v1", []trustKey{{"capacity-ci-v1", base64.StdEncoding.EncodeToString(publicKey)}}})
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	manifestFile, rootFile := filepath.Join(directory, "resource-capacity.json"), filepath.Join(directory, "release-trust-root.json")
	if err := os.WriteFile(manifestFile, manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rootFile, root, 0o600); err != nil {
		t.Fatal(err)
	}
	manifestDigest, rootDigest := sha256.Sum256(manifest), sha256.Sum256(root)
	return config.ResourceCapacityConfig{
		ManifestFile: manifestFile, TrustRootFile: rootFile,
		TrustRootSHA256: hex.EncodeToString(rootDigest[:]), ManifestSHA256: hex.EncodeToString(manifestDigest[:]),
		ArtifactID: "competition-resource-capacity", ArtifactLocator: "urn:sha256:" + hex.EncodeToString(manifestDigest[:]),
		GenerationID: signed.Manifest.GenerationID, Limits: signed.Manifest.Limits,
	}
}

func TestVerifyResourceCapacityAcceptsTMLSignedFall2026Manifest(t *testing.T) {
	configured := signedFall2026Capacity(t)
	verified, err := config.VerifyResourceCapacity(configured, "production")
	if err != nil {
		t.Fatal(err)
	}
	if verified.GenerationID != "forecast-capacity-2026-09-26" || verified.SignatureKeyID != "capacity-ci-v1" || verified.Limits.ForecastEventMaxInflight != 5 || verified.ArtifactLocator != configured.ArtifactLocator {
		t.Fatalf("verified capacity = %+v", verified)
	}
}

func TestResourceCapacityArtifactLocatorBindsContentAddress(t *testing.T) {
	for _, test := range []struct {
		name    string
		locator func(sha string) string
		want    string
	}{
		{name: "content address of the signed manifest", locator: func(sha string) string { return "urn:sha256:" + sha }},
		{name: "s3 location", locator: func(string) string { return "s3://forecast-artifacts/competition/resource-capacity.json" }},
		{name: "mismatched content address", locator: func(string) string { return "urn:sha256:" + strings.Repeat("0", 64) }, want: "must equal manifest_sha256"},
		{name: "short content address", locator: func(sha string) string { return "urn:sha256:" + sha[:63] }, want: "must carry 64 lowercase hex characters"},
		{name: "uppercase content address", locator: func(sha string) string { return "urn:sha256:" + strings.ToUpper(sha) }, want: "must carry 64 lowercase hex characters"},
		{name: "other urn", locator: func(sha string) string { return "urn:sha512:" + sha }, want: "explicit s3 URI or urn:sha256"},
		{name: "https location", locator: func(string) string { return "https://example.test/resource-capacity.json" }, want: "explicit s3 URI or urn:sha256"},
	} {
		t.Run(test.name, func(t *testing.T) {
			configured := signedFall2026Capacity(t)
			configured.ArtifactLocator = test.locator(configured.ManifestSHA256)
			_, err := config.VerifyResourceCapacity(configured, "production")
			if test.want == "" && err != nil || test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}
