package config_test

import (
	"slices"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
)

// The production hardening warning (#1559) names every reloadable credential
// configured as an environment variable, and nothing else.
func TestRotatableEnvSecretFieldsNamesReloadableEnvCredentials(t *testing.T) {
	env := func(name string) config.SecretRef { return config.SecretRef{Kind: config.SecretEnv, Name: name} }
	file := func(path string) config.SecretRef { return config.SecretRef{Kind: config.SecretFile, Path: path} }
	allEnv := func(c *config.Config) {
		c.State.Redis.Username = env("REDIS_USERNAME")
		c.State.Redis.Password = env("REDIS_PASSWORD")
		c.State.Redis.KeySecret = env("REDIS_KEY_SECRET")
		c.Langfuse = &config.LangfuseConfig{BaseURL: "https://cloud.langfuse.com", PublicKey: env("LANGFUSE_PUBLIC_KEY"), SecretKey: env("LANGFUSE_SECRET_KEY")}
		c.Continuation.HandleKeys = []config.HandleKey{
			{ID: "key-new", Primary: true, Secret: env("CONTINUATION_NEW")},
			{ID: "key-old", Secret: file("/var/run/secrets/continuation-old")},
			{ID: "key-older", Secret: env("CONTINUATION_OLDER")},
		}
		c.Temporal.PayloadCodec = &config.PayloadCodecConfig{Kind: config.PayloadCodecAES256GCM, Keys: []config.PayloadCodecKey{{ID: "codec", Primary: true, Secret: env("CODEC_KEY")}}}
		if c.State.Requests != nil {
			c.State.Requests.Secret = env("REQUESTS_SECRET")
		}
	}
	everyEnvField := []string{
		"state.redis.password",
		"langfuse.public_key",
		"langfuse.secret_key",
		"continuation.handle_keys[0].secret",
		"continuation.handle_keys[2].secret",
	}
	for _, test := range []struct {
		name   string
		mutate func(*config.Config)
		want   []string
	}{
		{name: "example configuration", mutate: func(*config.Config) {}},
		{name: "every reloadable credential from the environment", mutate: allEnv, want: everyEnvField},
		{name: "any non-development name is production", mutate: func(c *config.Config) { allEnv(c); c.Environment = "staging" }, want: everyEnvField},
		{name: "development", mutate: func(c *config.Config) { allEnv(c); c.Environment = config.DevelopmentEnvironment }},
		{name: "service mesh supplies no Redis credentials", mutate: func(c *config.Config) { allEnv(c); c.State.Redis.ServiceMesh = true }, want: everyEnvField[1:]},
		{name: "memory state ignores Redis", mutate: func(c *config.Config) { allEnv(c); c.State.Kind = config.StateKindMemory }, want: everyEnvField[1:]},
		{name: "file references", mutate: func(c *config.Config) {
			allEnv(c)
			c.State.Redis.Password = file("/var/run/secrets/redis-password")
			c.Langfuse.PublicKey, c.Langfuse.SecretKey = file("/var/run/secrets/langfuse-public"), file("/var/run/secrets/langfuse-secret")
			for index := range c.Continuation.HandleKeys {
				c.Continuation.HandleKeys[index].Secret = file("/var/run/secrets/continuation")
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := mustLoadConfig(t, exampleYAML(t))
			test.mutate(&c)
			if got := c.RotatableEnvSecretFields(); !slices.Equal(got, test.want) {
				t.Fatalf("RotatableEnvSecretFields() = %q, want %q", got, test.want)
			}
		})
	}
}
