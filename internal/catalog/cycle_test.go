package catalog

import (
	"context"
	"testing"
)

// Two streams that derive from each other form a cycle in the lineage graph.
// The lineage query must still terminate and report each stream in the other's
// downstream set without looping.
func TestLineageHandlesDerivationCycle(t *testing.T) {
	svc := newTestService()
	ctx := context.Background()

	a, err := svc.Register(ctx, sampleInput("stream-a"), "team-pay")
	if err != nil {
		t.Fatalf("register a: %v", err)
	}
	b, err := svc.Register(ctx, sampleInput("stream-b"), "team-pay")
	if err != nil {
		t.Fatalf("register b: %v", err)
	}

	if derErr := svc.AddDerivation(ctx, a.ID, b.ID); derErr != nil {
		t.Fatalf("derive a->b: %v", derErr)
	}
	if derErr := svc.AddDerivation(ctx, b.ID, a.ID); derErr != nil {
		t.Fatalf("derive b->a: %v", derErr)
	}

	view, err := svc.Lineage(ctx, a.ID)
	if err != nil {
		t.Fatalf("lineage: %v", err)
	}
	if !contains(view.Downstream, "stream:"+b.ID) {
		t.Fatalf("expected stream:%s downstream of a, got %v", b.ID, view.Downstream)
	}
	// The cycle means a reaches itself, but it must not list itself.
	if contains(view.Downstream, "stream:"+a.ID) {
		t.Fatal("stream must not appear in its own downstream set")
	}
}

func contains(xs []string, target string) bool {
	for _, x := range xs {
		if x == target {
			return true
		}
	}
	return false
}
