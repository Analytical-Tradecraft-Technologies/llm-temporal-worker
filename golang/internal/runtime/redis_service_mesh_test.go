package runtime

import (
	"context"
	"github.com/mfow/llm-temporal-worker/golang/config"
	redis "github.com/redis/go-redis/v9"
	"testing"
)

func TestBuildRedisServiceMeshSkipsCredentialsAndTLS(t *testing.T) {
	called := false
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		RedisFactory: func(_ context.Context, value config.RedisConfig, username, password string) (redis.UniversalClient, error) {
			called = true
			if username != "" || password != "" || value.TLS.Enabled {
				t.Fatal("mesh client received application credentials or TLS")
			}
			return redis.NewClient(&redis.Options{Addr: "localhost:6379"}), nil
		},
	}}
	client, owned, err := factory.buildRedis(context.Background(), config.Config{State: config.StateConfig{Redis: config.RedisConfig{ServiceMesh: true}}})
	if err != nil || !called || !owned {
		t.Fatalf("called=%v owned=%v error=%v", called, owned, err)
	}
	defer client.Close()
}
