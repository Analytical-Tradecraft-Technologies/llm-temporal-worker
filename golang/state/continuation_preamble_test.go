package state

import (
	"context"
	"errors"
	"testing"
)

func TestIssueRootChecksTenantAndResetsDepth(t *testing.T) {
	keyring, err := NewKeyring([]Key{{ID: "key-1", Secret: []byte("01234567890123456789012345678901"), Primary: true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := IssueRoot(context.Background(), keyring, Continuation{}); err == nil || err.Error() != "continuation tenant is required" {
		t.Fatalf("missing tenant = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := IssueRoot(canceled, keyring, Continuation{Tenant: "tenant"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}
	handle, root, err := IssueRoot(context.Background(), keyring, Continuation{Tenant: "tenant", Depth: 3})
	if err != nil || handle == "" || root.ID != handle.String() || root.Depth != 0 {
		t.Fatalf("root = %q %#v, %v", handle, root, err)
	}
}

func TestPrepareChildInheritsTenantAndRejectsMismatches(t *testing.T) {
	parent := Continuation{Tenant: "tenant", Depth: 1}
	get := func(_ context.Context, handle Handle) (Continuation, error) {
		if handle != "parent" {
			return Continuation{}, ErrNotFound
		}
		return parent, nil
	}
	request := PutChildRequest{Parent: "parent", Child: Continuation{Depth: 2, ParentID: "parent"}}
	gotParent, child, err := PrepareChild(context.Background(), get, request, 4)
	if err != nil || gotParent.Tenant != "tenant" || child.Tenant != "tenant" {
		t.Fatalf("child = %#v, %v", child, err)
	}
	for name, test := range map[string]struct {
		request  PutChildRequest
		maxDepth int
		want     string
	}{
		"missing parent": {PutChildRequest{Parent: "other"}, 4, ErrNotFound.Error()},
		"other tenant":   {PutChildRequest{Parent: "parent", Child: Continuation{Tenant: "other", Depth: 2, ParentID: "parent"}}, 4, ErrConflict.Error()},
		"wrong depth":    {PutChildRequest{Parent: "parent", Child: Continuation{Depth: 3, ParentID: "parent"}}, 4, ErrConflict.Error()},
		"wrong parent":   {PutChildRequest{Parent: "parent", Child: Continuation{Depth: 2, ParentID: "other"}}, 4, ErrConflict.Error()},
		"too deep":       {request, 1, "continuation depth exceeds limit"},
	} {
		if _, _, err := PrepareChild(context.Background(), get, test.request, test.maxDepth); err == nil || err.Error() != test.want {
			t.Errorf("%s = %v, want %s", name, err, test.want)
		}
	}
}

func TestSameChildFactsComparesHistory(t *testing.T) {
	stored := Continuation{TranscriptDigest: [32]byte{1}, TranscriptComplete: true}
	if !SameChildFacts(stored, stored) {
		t.Fatal("identical facts differ")
	}
	if SameChildFacts(stored, Continuation{TranscriptDigest: [32]byte{2}, TranscriptComplete: true}) || SameChildFacts(stored, Continuation{TranscriptDigest: [32]byte{1}}) {
		t.Fatal("different history matched")
	}
}
