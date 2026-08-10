package integration_test

import (
	"fmt"
	"strings"
	"testing"
)

func validateHardenedTmpfsOptions(mounts map[string]string) error {
	if len(mounts) != 1 {
		return fmt.Errorf("want exactly one writable mount at /tmp")
	}
	options, ok := mounts["/tmp"]
	if !ok {
		return fmt.Errorf("sole writable mount must be /tmp")
	}
	required := map[string]bool{
		"rw":       false,
		"nosuid":   false,
		"nodev":    false,
		"noexec":   false,
		"size=64m": false,
	}
	optional := map[string]struct{}{
		"rprivate":  {},
		"tmpcopyup": {},
	}
	seen := make(map[string]struct{}, len(required)+len(optional))
	for _, option := range strings.Split(options, ",") {
		if _, duplicate := seen[option]; duplicate {
			return fmt.Errorf("tmpfs option %q is repeated", option)
		}
		seen[option] = struct{}{}
		if _, ok := required[option]; ok {
			required[option] = true
			continue
		}
		if _, ok := optional[option]; ok {
			continue
		}
		return fmt.Errorf("tmpfs option %q is not allowed", option)
	}
	for option, present := range required {
		if !present {
			return fmt.Errorf("required tmpfs option %q is missing", option)
		}
	}
	return nil
}

func TestValidateHardenedTmpfsOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		mounts    map[string]string
		wantError bool
	}{
		{name: "Docker", mounts: map[string]string{"/tmp": "rw,nosuid,nodev,noexec,size=64m"}},
		{name: "Podman safe additions", mounts: map[string]string{"/tmp": "rw,nosuid,nodev,noexec,size=64m,rprivate,tmpcopyup"}},
		{name: "token order independent", mounts: map[string]string{"/tmp": "tmpcopyup,size=64m,noexec,nodev,nosuid,rw,rprivate"}},
		{name: "missing noexec", mounts: map[string]string{"/tmp": "rw,nosuid,nodev,size=64m"}, wantError: true},
		{name: "wrong size", mounts: map[string]string{"/tmp": "rw,nosuid,nodev,noexec,size=128m"}, wantError: true},
		{name: "unsafe exec", mounts: map[string]string{"/tmp": "rw,nosuid,nodev,noexec,size=64m,exec"}, wantError: true},
		{name: "unknown propagation", mounts: map[string]string{"/tmp": "rw,nosuid,nodev,noexec,size=64m,rshared"}, wantError: true},
		{name: "duplicate token", mounts: map[string]string{"/tmp": "rw,rw,nosuid,nodev,noexec,size=64m"}, wantError: true},
		{name: "additional mount", mounts: map[string]string{"/tmp": "rw,nosuid,nodev,noexec,size=64m", "/data": "rw"}, wantError: true},
		{name: "wrong mount", mounts: map[string]string{"/var/tmp": "rw,nosuid,nodev,noexec,size=64m"}, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validateHardenedTmpfsOptions(test.mounts)
			if (err != nil) != test.wantError {
				t.Fatalf("validateHardenedTmpfsOptions(%#v) error = %v, want error %t", test.mounts, err, test.wantError)
			}
		})
	}
}
