package config

import "fmt"

// RotatableEnvSecretFields lists the secret references that a production
// worker re-reads on every configuration reload but that are configured as
// `kind: env`. A process cannot see a new value for its own environment
// variable, so rotating one of these credentials needs a restart; a `file`
// reference is rotated by replacing the file and sending SIGHUP.
//
// Only credentials whose clients are rebuilt for each snapshot are listed:
// the Redis password, the Langfuse key pair and the continuation handle keys.
// Durable identity secrets (state.redis.key_secret, state.requests.secret)
// are never rotated in place, temporal.payload_codec keys are process-lifetime
// whatever their kind, and provider endpoint auth has no file form, so none of
// them is listed. The result names schema paths only, never a reference name
// or value, and is empty outside production.
func (config Config) RotatableEnvSecretFields() []string {
	if !IsProductionEnvironment(config.Environment) {
		return nil
	}
	var fields []string
	add := func(path string, ref SecretRef) {
		if ref.Kind == SecretEnv {
			fields = append(fields, path)
		}
	}
	// Mirrors the references the worker resolves for Redis: memory state
	// ignores the section and a service mesh supplies no credentials.
	if config.State.Kind != StateKindMemory && !config.State.Redis.ServiceMesh {
		add("state.redis.password", config.State.Redis.Password)
	}
	if config.Langfuse != nil {
		add("langfuse.public_key", config.Langfuse.PublicKey)
		add("langfuse.secret_key", config.Langfuse.SecretKey)
	}
	for index, key := range config.Continuation.HandleKeys {
		add(fmt.Sprintf("continuation.handle_keys[%d].secret", index), key.Secret)
	}
	return fields
}
