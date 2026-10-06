package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"testing"
	"time"
)

func TestLangfuseCaptureSurvivesRestartAndAcknowledgment(t *testing.T) {
	r, table, blobs, req := fixture(t)
	ctx := context.Background()
	if _, err := r.Create(ctx, req); err != nil {
		t.Fatal(err)
	}
	data := json.RawMessage(`{"input":"private trace prompt"}`)
	if err := r.SaveLangfuseCapture(ctx, req.Scope, req.ID, "attempt", data); err != nil {
		t.Fatal(err)
	}
	for _, payload := range blobs.values {
		if bytes.Contains(payload, []byte("private trace prompt")) {
			t.Fatal("capture plaintext leaked to blob storage")
		}
	}
	if err := r.SaveLangfuseCapture(ctx, req.Scope, req.ID, "attempt", json.RawMessage(`{"input":"different"}`)); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("immutable capture overwritten: %v", err)
	}
	r = reopen(t, table, blobs)
	captures, err := r.LoadLangfuseCaptures(ctx, req.Scope, req.ID)
	if err != nil || string(captures["attempt"]) != string(data) {
		t.Fatalf("capture not retained: %v", err)
	}
	if _, err := r.LoadLangfuseCaptures(ctx, Scope{Tenant: "other", Project: req.Scope.Project}, req.ID); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("cross-scope leak: %v", err)
	}
	now := time.Now()
	owner, err := r.ClaimLangfuseExport(ctx, req.Scope, req.ID, now)
	if err != nil || owner == "" {
		t.Fatal("claim failed", err)
	}
	if other, err := r.ClaimLangfuseExport(ctx, req.Scope, req.ID, now); err == nil || other != "" {
		t.Fatal("concurrent export permitted")
	}
	if err := r.AcknowledgeLangfuseExport(ctx, req.Scope, req.ID, owner); err != nil {
		t.Fatal(err)
	}
	owner, err = reopen(t, table, blobs).ClaimLangfuseExport(ctx, req.Scope, req.ID, now.Add(time.Minute))
	if err != nil || owner != "" {
		t.Fatal("acknowledged export repeated", err)
	}
}
