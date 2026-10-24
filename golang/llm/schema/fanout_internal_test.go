package schema

import "testing"

// Property names are caller text, so a path can embed any delimiter; bucket
// signatures must still tell different multisets apart.
func TestFanoutBucketSignatureIsUnambiguous(t *testing.T) {
	a := &fanoutNode{id: 1, path: "/properties/a"}
	b := &fanoutNode{id: 2, path: "/properties/b"}
	crafted := &fanoutNode{id: 3, path: a.path + "\x001\x01" + b.path}
	pair := fanoutBucket{nodes: map[*fanoutNode]int{a: 1, b: 1}}
	single := fanoutBucket{nodes: map[*fanoutNode]int{crafted: 1}}
	if pair.signature() == single.signature() {
		t.Fatalf("different buckets share signature %q", pair.signature())
	}
	again := fanoutBucket{nodes: map[*fanoutNode]int{b: 1, a: 1}}
	if pair.signature() != again.signature() {
		t.Fatal("equal buckets have different signatures")
	}
	if (fanoutBucket{nodes: map[*fanoutNode]int{a: 2}}).signature() == (fanoutBucket{nodes: map[*fanoutNode]int{a: 1}}).signature() {
		t.Fatal("counts are not part of the signature")
	}
}
