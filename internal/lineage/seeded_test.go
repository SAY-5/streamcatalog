package lineage

import (
	"reflect"
	"testing"
)

// seededGraph models a realistic lineage with a diamond and a back edge that
// forms a cycle:
//
//	team:src -> stream:raw -> stream:clean -> stream:enriched -> team:ml
//	                       \-> stream:metrics -> team:dash
//	stream:enriched -> stream:raw   (cycle back to raw)
func seededGraph() *Graph {
	return Build([]Edge{
		{From: "team:src", To: "stream:raw"},
		{From: "stream:raw", To: "stream:clean"},
		{From: "stream:clean", To: "stream:enriched"},
		{From: "stream:enriched", To: "team:ml"},
		{From: "stream:raw", To: "stream:metrics"},
		{From: "stream:metrics", To: "team:dash"},
		{From: "stream:enriched", To: "stream:raw"},
	})
}

func TestSeededTransitiveDownstream(t *testing.T) {
	g := seededGraph()
	got := g.Downstream("stream:raw")
	want := []string{
		"stream:clean", "stream:enriched", "stream:metrics",
		"team:dash", "team:ml",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("downstream(stream:raw) = %v, want %v", got, want)
	}
}

func TestSeededTransitiveUpstream(t *testing.T) {
	g := seededGraph()
	got := g.Upstream("team:ml")
	// team:ml is fed by enriched, which is fed by clean, raw, and through the
	// cycle by enriched itself; src and the metrics branch also reach raw.
	want := []string{
		"stream:clean", "stream:enriched", "stream:raw", "team:src",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("upstream(team:ml) = %v, want %v", got, want)
	}
}

func TestSeededGraphHasCycle(t *testing.T) {
	if !seededGraph().HasCycle() {
		t.Fatal("seeded graph should contain a cycle")
	}
}

// Regression: a cycle must not cause the start node to appear in its own
// downstream set even though it is reachable from itself through the back edge.
func TestCycleDoesNotSelfInclude(t *testing.T) {
	g := seededGraph()
	for _, n := range g.Downstream("stream:raw") {
		if n == "stream:raw" {
			t.Fatal("start node must not appear in its own downstream set")
		}
	}
}
