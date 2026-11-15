package blob

import (
	"errors"
	"testing"
	"time"
)

func TestPutRequestCheckReadsClockOnlyForWellFormedRequests(t *testing.T) {
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	reads := 0
	clock := func() time.Time { reads++; return now }
	valid := PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("hello"), ExpiresAt: now.Add(time.Hour)}

	for name, request := range map[string]PutRequest{
		"missing tenant": {MediaType: "text/plain", Data: []byte("x"), ExpiresAt: now.Add(time.Hour)},
		"too large":      {Tenant: "tenant", MediaType: "text/plain", Data: []byte("too large"), ExpiresAt: now.Add(time.Hour)},
	} {
		if _, _, _, err := request.Check(5, clock); err == nil {
			t.Fatalf("%s: want error", name)
		}
	}
	if reads != 0 {
		t.Fatalf("clock read %d times for malformed requests", reads)
	}
	expired := valid
	expired.ExpiresAt = now
	if _, _, _, err := expired.Check(5, clock); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired = %v, want ErrExpired", err)
	}

	prefix, digest, checkedAt, err := valid.Check(5, clock)
	wantPrefix, _ := TenantPrefix("tenant")
	if err != nil || prefix != wantPrefix || digest != Digest(valid.Data) || !checkedAt.Equal(now) || reads != 2 {
		t.Fatalf("check = %q, %q, %v, %v after %d reads", prefix, digest, checkedAt, err, reads)
	}
	ref := valid.Ref("memory", prefix+"/"+digest, digest)
	if ref.Store != "memory" || ref.Locator != prefix+"/"+digest || ref.Digest != digest || ref.ByteLength != 5 || ref.MediaType != "text/plain" || !ref.ExpiresAt.Equal(valid.ExpiresAt) {
		t.Fatalf("ref = %#v", ref)
	}
}
