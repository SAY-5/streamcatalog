package lineage

import (
	"reflect"
	"testing"
)

func TestDownstreamAndUpstream(t *testing.T) {
	// team:a -> stream:1 -> stream:2 -> team:b
	g := Build([]Edge{
		{From: "team:a", To: "stream:1"},
		{From: "stream:1", To: "stream:2"},
		{From: "stream:2", To: "team:b"},
	})

	got := g.Downstream("stream:1")
	want := []string{"stream:2", "team:b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("downstream = %v, want %v", got, want)
	}

	gotUp := g.Upstream("stream:2")
	wantUp := []string{"stream:1", "team:a"}
	if !reflect.DeepEqual(gotUp, wantUp) {
		t.Fatalf("upstream = %v, want %v", gotUp, wantUp)
	}
}

func TestCycleTerminatesTraversal(t *testing.T) {
	g := Build([]Edge{
		{From: "stream:1", To: "stream:2"},
		{From: "stream:2", To: "stream:3"},
		{From: "stream:3", To: "stream:1"},
	})

	if !g.HasCycle() {
		t.Fatal("expected cycle to be detected")
	}
	// Traversal must terminate and report the other two nodes.
	got := g.Downstream("stream:1")
	want := []string{"stream:2", "stream:3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("downstream with cycle = %v, want %v", got, want)
	}
}

func TestAcyclicGraphHasNoCycle(t *testing.T) {
	g := Build([]Edge{
		{From: "stream:1", To: "stream:2"},
		{From: "stream:2", To: "stream:3"},
	})
	if g.HasCycle() {
		t.Fatal("did not expect a cycle")
	}
}
