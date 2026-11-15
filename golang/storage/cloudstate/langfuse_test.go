package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"testing"
	"time"
)

func TestLangfuseCaptureSurvivesRestartAndAcknowledgment(t *testing.T) {
	r, table, blobs, req := fixture(t)
	ctx := context.Background()
	mustCreate(t, r, req)
	data := json.RawMessage(`{"input":"private trace prompt"}`)
	mustNoError(t, r.SaveLangfuseCapture(ctx, req.Scope, req.ID, "attempt", data))
	for _, payload := range blobs.values {
		if bytes.Contains(payload, []byte("private trace prompt")) {
			t.Fatal("capture plaintext leaked to blob storage")
		}
	}
	requireErrorIs(t, r.SaveLangfuseCapture(ctx, req.Scope, req.ID, "attempt", json.RawMessage(`{"input":"different"}`)), contracts.ErrConflict, "immutable capture overwritten")
	r = reopen(t, table, blobs)
	captures, err := r.LoadLangfuseCaptures(ctx, req.Scope, req.ID)
	if err != nil || string(captures["attempt"]) != string(data) {
		t.Fatalf("capture not retained: %v", err)
	}
	requireErrorIs(t, errOf(r.LoadLangfuseCaptures(ctx, Scope{Tenant: "other", Project: req.Scope.Project}, req.ID)), contracts.ErrNotFound, "cross-scope leak")
	now := time.Now()
	owner, err := r.ClaimLangfuseExport(ctx, req.Scope, req.ID, now)
	if err != nil || owner == "" {
		t.Fatal("claim failed", err)
	}
	if other, err := r.ClaimLangfuseExport(ctx, req.Scope, req.ID, now); err == nil || other != "" {
		t.Fatal("concurrent export permitted")
	}
	mustNoError(t, r.AcknowledgeLangfuseExport(ctx, req.Scope, req.ID, owner))
	owner, err = reopen(t, table, blobs).ClaimLangfuseExport(ctx, req.Scope, req.ID, now.Add(time.Minute))
	if err != nil || owner != "" {
		t.Fatal("acknowledged export repeated", err)
	}
}
