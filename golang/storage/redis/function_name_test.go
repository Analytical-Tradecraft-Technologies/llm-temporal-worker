package redis

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"regexp"
	"strings"
	"testing"
)

// The Function names carry the Lua source digest, so a release that changes
// the Lua loads a new library beside the old one instead of replacing it
// (#1006). The shipped configurations must pin this release's identity.
func TestAdmissionFunctionNamesCarryTheLuaSourceDigest(t *testing.T) {
	digest := sha256.Sum256([]byte(admissionFunctionSource))
	tag := hex.EncodeToString(digest[:8])
	if AdmissionFunctionLibrary != "llmtw_admission_"+tag || AdmissionFunctionVersion != "admission_"+tag {
		t.Fatalf("names = %q/%q, want the %s source tag", AdmissionFunctionLibrary, AdmissionFunctionVersion, tag)
	}
	identifier := regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	if !identifier.MatchString(AdmissionFunctionLibrary) || !identifier.MatchString(AdmissionFunctionVersion) {
		t.Fatal("Function names are not valid Redis Function identifiers")
	}
	if !strings.HasPrefix(admissionFunctionLibrarySource, "#!lua name="+AdmissionFunctionLibrary+"\n") || !strings.Contains(admissionFunctionLibrarySource, "redis.register_function('"+AdmissionFunctionVersion+"'") {
		t.Fatal("library source does not register the derived names")
	}
	for _, path := range []string{"../../config.example.yaml", "../../deploy/local/config.yaml", "../../deploy/kubernetes/base/config.yaml"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"function_library: " + AdmissionFunctionLibrary, "admission_version: " + AdmissionFunctionVersion, "admission_digest: " + AdmissionFunctionDigest()} {
			if !strings.Contains(string(data), want) {
				t.Errorf("%s does not pin %q", path, want)
			}
		}
	}
}
