package runtime

import (
	"context"
	"testing"
)

func TestEphemeralRedisProbeRequiresExactSafePolicies(t *testing.T) {
	for _, test := range []struct {
		name, policy, aof, save string
		ready                   bool
	}{
		{"ephemeral protected accounting", "volatile-lru", "no", "", true},
		{"AOF still enabled", "volatile-lru", "yes", "", false},
		{"snapshots still enabled", "volatile-lru", "no", "60 1", false},
		{"all keys evictable", "allkeys-lru", "no", "", false},
		{"eviction missing", "noeviction", "no", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := testRedisProbeConfig()
			value.RequiredPersistence, value.MaxMemoryPolicy = "none", "volatile-lru"
			client := healthyRedisProbeClient()
			client.settings["maxmemory-policy"] = test.policy
			client.settings["appendonly"] = test.aof
			client.settings["save"] = test.save
			probe, err := NewRedisDependencyProbe(client, value)
			if err != nil {
				t.Fatal(err)
			}
			if result := probe.Probe(context.Background()); (result.Status == ProbeStatusReady) != test.ready {
				t.Fatalf("probe result = %#v", result)
			}
			if client.mutations != 0 {
				t.Fatal("readiness modified Redis")
			}
		})
	}
}
