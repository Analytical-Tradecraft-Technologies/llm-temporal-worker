package config

import (
	"fmt"
	"sort"
)

// WorkloadIdentityPaths lists every secret reference and endpoint auth mode
// that uses workload_identity. The kind is part of the configuration
// contract for embeddings that supply a workload identity provider; the
// worker binary does not, so its commands reject these paths up front.
func (config Config) WorkloadIdentityPaths() []string {
	var paths []string
	add := func(kind, path string) {
		if kind == string(SecretWorkloadIdentity) {
			paths = append(paths, path)
		}
	}
	add(string(config.State.Redis.Username.Kind), "state.redis.username")
	add(string(config.State.Redis.Password.Kind), "state.redis.password")
	add(string(config.State.Redis.KeySecret.Kind), "state.redis.key_secret")
	if config.State.Requests != nil {
		add(string(config.State.Requests.Secret.Kind), "state.requests.secret")
	}
	for index, key := range config.Continuation.HandleKeys {
		add(string(key.Secret.Kind), fmt.Sprintf("continuation.handle_keys[%d].secret", index))
	}
	if config.Temporal.PayloadCodec != nil {
		for index, key := range config.Temporal.PayloadCodec.Keys {
			add(string(key.Secret.Kind), fmt.Sprintf("temporal.payload_codec.keys[%d].secret", index))
		}
	}
	add(config.BlobStore.S3.Auth.Kind, "blob_store.s3.auth")
	for name, endpoint := range config.Endpoints {
		add(endpoint.Auth.Kind, "endpoints."+name+".auth")
	}
	sort.Strings(paths)
	return paths
}
