package lineage

import (
	"fmt"
	"testing"
)

// buildChainWithBranches produces a graph of n streams in a chain where each
// stream also fans out to two consumer nodes, modelling a deep lineage with
// branching downstream consumers.
func buildChainWithBranches(n int) []Edge {
	edges := make([]Edge, 0, n*3)
	for i := 0; i < n; i++ {
		cur := fmt.Sprintf("stream:%d", i)
		if i+1 < n {
			edges = append(edges, Edge{From: cur, To: fmt.Sprintf("stream:%d", i+1)})
		}
		edges = append(edges,
			Edge{From: cur, To: fmt.Sprintf("team:c%d-a", i)},
			Edge{From: cur, To: fmt.Sprintf("team:c%d-b", i)},
		)
	}
	return edges
}

func BenchmarkDownstreamTraversal(b *testing.B) {
	edges := buildChainWithBranches(2000)
	g := Build(edges)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.Downstream("stream:0")
	}
}

func BenchmarkBuildAndTraverse(b *testing.B) {
	edges := buildChainWithBranches(2000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g := Build(edges)
		_ = g.Downstream("stream:0")
	}
}
