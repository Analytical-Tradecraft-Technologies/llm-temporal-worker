package config

import (
	"fmt"
	"strings"
	"time"
)

type RegionalBucket struct {
	Region string `yaml:"region" json:"region"`
	Bucket string `yaml:"bucket" json:"bucket"`
}
type CloudFailoverConfig struct {
	DynamoDBRegions []string         `yaml:"dynamodb_regions" json:"dynamodb_regions"`
	PayloadReplicas []RegionalBucket `yaml:"payload_replicas" json:"payload_replicas"`
	AttemptTimeout  Duration         `yaml:"attempt_timeout" json:"attempt_timeout"`
}
type BlobFailoverConfig struct {
	Replicas       []RegionalBucket `yaml:"replicas" json:"replicas"`
	AttemptTimeout Duration         `yaml:"attempt_timeout" json:"attempt_timeout"`
}

func validateRegionalBuckets(primaryRegion, primaryBucket string, replicas []RegionalBucket) error {
	if len(replicas) < 1 || len(replicas) > 2 {
		return fmt.Errorf("regional storage requires one or two fallback buckets")
	}
	regions := map[string]bool{primaryRegion: true}
	buckets := map[string]bool{primaryBucket: true}
	for _, replica := range replicas {
		if strings.TrimSpace(replica.Region) == "" || strings.TrimSpace(replica.Region) != replica.Region || strings.TrimSpace(replica.Bucket) == "" || strings.TrimSpace(replica.Bucket) != replica.Bucket || regions[replica.Region] || buckets[replica.Bucket] {
			return fmt.Errorf("regional storage requires distinct nonempty regions and buckets")
		}
		regions[replica.Region] = true
		buckets[replica.Bucket] = true
	}
	return nil
}
func validateAttemptTimeout(timeout Duration) error {
	if time.Duration(timeout) <= 0 || time.Duration(timeout) > 30*time.Second {
		return fmt.Errorf("regional attempt_timeout must be positive and at most 30s")
	}
	return nil
}
