//go:build integration

package redis

import "testing"

func TestLiveRedisBudgetWorkerRegistrationRejectsInterveningWrites(t *testing.T) {
	client := openLiveRedis(t)
	runBudgetWorkerRegistrationInterleavings(t, func(t *testing.T) (BudgetWorkerLeaseRedisClient, BudgetKeySpace) {
		options := liveKeyOptions("worker-register")
		cleanupLivePrefix(t, client, options.Prefix)
		keys, err := NewBudgetKeySpace(options)
		if err != nil {
			t.Fatal(err)
		}
		return client, keys
	})
}
