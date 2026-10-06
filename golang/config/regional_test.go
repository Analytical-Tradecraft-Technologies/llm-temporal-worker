package config_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm/schema"
)

func TestRegionalConfigValidation(t *testing.T) {
	makeConfig := func() config.Config {
		value, err := config.Load(exampleYAML(t))
		if err != nil {
			t.Fatal(err)
		}
		value.State.Requests.Provider.AWS.Region = "region-primary"
		value.State.Requests.Provider.AWS.AllowMRSC = true
		value.State.Requests.Provider.AWS.Failover = &config.CloudFailoverConfig{DynamoDBRegions: []string{"region-fallback-a", "region-fallback-b"}, PayloadReplicas: []config.RegionalBucket{{Region: "region-fallback-a", Bucket: "payloads-replica"}}, AttemptTimeout: config.Duration(5 * time.Second)}
		value.BlobStore.S3.Region = "region-primary"
		value.BlobStore.S3.Failover = &config.BlobFailoverConfig{Replicas: []config.RegionalBucket{{Region: "region-fallback-a", Bucket: "results-replica"}}, AttemptTimeout: config.Duration(5 * time.Second)}
		return value
	}
	value := makeConfig()
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.Validate(data); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*config.Config)
	}{
		{"MRSC required", func(c *config.Config) { c.State.Requests.Provider.AWS.AllowMRSC = false }},
		{"duplicate region", func(c *config.Config) { c.State.Requests.Provider.AWS.Failover.DynamoDBRegions[0] = "region-primary" }},
		{"empty endpoint", func(c *config.Config) { c.State.Requests.Provider.AWS.Failover.DynamoDBRegions[0] = "" }},
		{"same payload bucket", func(c *config.Config) {
			c.State.Requests.Provider.AWS.Failover.PayloadReplicas[0].Bucket = c.State.Requests.Provider.BlobStores[c.State.Requests.PayloadStore]
		}},
		{"timeout required", func(c *config.Config) { c.State.Requests.Provider.AWS.Failover.AttemptTimeout = 0 }},
		{"timeout bounded", func(c *config.Config) { c.BlobStore.S3.Failover.AttemptTimeout = config.Duration(time.Minute) }},
		{"result region distinct", func(c *config.Config) { c.BlobStore.S3.Failover.Replicas[0].Region = "region-primary" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := makeConfig()
			tc.change(&value)
			if err := value.Validate(); err == nil {
				t.Fatal("invalid failover configuration accepted")
			}
		})
	}
}
