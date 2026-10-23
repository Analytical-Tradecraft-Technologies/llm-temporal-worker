package config

import "testing"

func TestValidateKeyRingHandleKeyMessages(t *testing.T) {
	secret := SecretRef{Kind: SecretEnv, Name: "HANDLE_KEY"}
	for name, test := range map[string]struct {
		keys []HandleKey
		want string
	}{
		"empty":         {keys: nil, want: "continuation.handle_keys must not be empty"},
		"invalid id":    {keys: []HandleKey{{ID: "key.one", Primary: true, Secret: secret}}, want: "continuation.handle_keys[0].id must be 1-32 characters from [A-Za-z0-9_-]"},
		"duplicate id":  {keys: []HandleKey{{ID: "k1", Primary: true, Secret: secret}, {ID: "k1", Secret: secret}}, want: `continuation.handle_keys[1] duplicate key ID "k1"`},
		"no primary":    {keys: []HandleKey{{ID: "k1", Secret: secret}}, want: "continuation.handle_keys must contain exactly one primary key"},
		"two primaries": {keys: []HandleKey{{ID: "k1", Primary: true, Secret: secret}, {ID: "k2", Primary: true, Secret: secret}}, want: "continuation.handle_keys must contain exactly one primary key"},
	} {
		err := validateKeyRing(test.keys, "continuation.handle_keys", continuationHandleKeyIDPattern, "1-32 characters from [A-Za-z0-9_-]")
		if err == nil || err.Error() != test.want {
			t.Errorf("%s: validateKeyRing() = %v, want %q", name, err, test.want)
		}
	}
	valid := []HandleKey{{ID: "k1", Primary: true, Secret: secret}, {ID: "k2", Secret: secret}}
	if err := validateKeyRing(valid, "continuation.handle_keys", continuationHandleKeyIDPattern, "1-32 characters from [A-Za-z0-9_-]"); err != nil {
		t.Fatalf("valid key ring rejected: %v", err)
	}
}
