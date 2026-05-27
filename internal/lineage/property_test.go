package lineage

import (
	"fmt"
	"testing"
	"testing/quick"
)

// Property: for any set of edges, every node returned by Downstream and
// Upstream is a node that appears in the graph, and the start node is never
// included in its own result. This guards against dangling references and
// self-inclusion across arbitrary inputs.
func TestTraversalResultsAreConsistentNodes(t *testing.T) {
	prop := func(pairs [][2]uint8) bool {
		edges := make([]Edge, 0, len(pairs))
		for _, p := range pairs {
			edges = append(edges, Edge{
				From: fmt.Sprintf("n%d", p[0]),
				To:   fmt.Sprintf("n%d", p[1]),
			})
		}
		g := Build(edges)
		nodes := map[string]struct{}{}
		for _, n := range g.Nodes() {
			nodes[n] = struct{}{}
		}
		for n := range nodes {
			for _, dir := range [][]string{g.Downstream(n), g.Upstream(n)} {
				for _, reached := range dir {
					if reached == n {
						return false
					}
					if _, ok := nodes[reached]; !ok {
						return false
					}
				}
			}
		}
		return true
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 300}); err != nil {
		t.Fatalf("property failed: %v", err)
	}
}

// Property: downstream of a node is closed under the edge relation, meaning a
// node reachable in one step from any downstream node is itself downstream.
func TestDownstreamIsTransitivelyClosed(t *testing.T) {
	prop := func(pairs [][2]uint8) bool {
		edges := make([]Edge, 0, len(pairs))
		for _, p := range pairs {
			edges = append(edges, Edge{From: fmt.Sprintf("n%d", p[0]), To: fmt.Sprintf("n%d", p[1])})
		}
		g := Build(edges)
		for _, start := range g.Nodes() {
			down := map[string]struct{}{}
			for _, d := range g.Downstream(start) {
				down[d] = struct{}{}
			}
			for d := range down {
				for next := range g.out[d] {
					if next == start {
						continue
					}
					if _, ok := down[next]; !ok {
						return false
					}
				}
			}
		}
		return true
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 300}); err != nil {
		t.Fatalf("property failed: %v", err)
	}
}
