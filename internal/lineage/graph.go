// Package lineage builds a directed graph from lineage edges and answers
// upstream/downstream reachability queries with cycle handling.
package lineage

import "sort"

// Edge is a directed connection between two lineage nodes.
type Edge struct {
	From string
	To   string
}

// Graph is an in-memory adjacency representation of lineage edges.
type Graph struct {
	out map[string]map[string]struct{}
	in  map[string]map[string]struct{}
}

// Build constructs a graph from a set of edges. Each edge connects From to To.
// Producers, streams, and consumers are all nodes.
func Build(edges []Edge) *Graph {
	g := &Graph{
		out: make(map[string]map[string]struct{}),
		in:  make(map[string]map[string]struct{}),
	}
	for _, e := range edges {
		g.addEdge(e.From, e.To)
	}
	return g
}

func (g *Graph) addEdge(from, to string) {
	if g.out[from] == nil {
		g.out[from] = make(map[string]struct{})
	}
	if g.in[to] == nil {
		g.in[to] = make(map[string]struct{})
	}
	g.out[from][to] = struct{}{}
	g.in[to][from] = struct{}{}
}

// Downstream returns every node reachable from start by following edges
// forward. The start node is not included. Cycles are handled via the visited
// set so traversal always terminates.
func (g *Graph) Downstream(start string) []string {
	return g.traverse(start, g.out)
}

// Upstream returns every node that can reach start by following edges backward.
func (g *Graph) Upstream(start string) []string {
	return g.traverse(start, g.in)
}

func (g *Graph) traverse(start string, adj map[string]map[string]struct{}) []string {
	visited := map[string]struct{}{start: {}}
	result := map[string]struct{}{}
	stack := []string{start}
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for next := range adj[node] {
			if _, seen := visited[next]; seen {
				continue
			}
			visited[next] = struct{}{}
			result[next] = struct{}{}
			stack = append(stack, next)
		}
	}
	out := make([]string, 0, len(result))
	for n := range result {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// HasCycle reports whether the graph contains a directed cycle.
func (g *Graph) HasCycle() bool {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int)
	nodes := g.Nodes()
	var visit func(string) bool
	visit = func(n string) bool {
		color[n] = gray
		for next := range g.out[n] {
			switch color[next] {
			case gray:
				return true
			case white:
				if visit(next) {
					return true
				}
			}
		}
		color[n] = black
		return false
	}
	for _, n := range nodes {
		if color[n] == white {
			if visit(n) {
				return true
			}
		}
	}
	return false
}

// Nodes returns every node in the graph, sorted.
func (g *Graph) Nodes() []string {
	set := map[string]struct{}{}
	for n := range g.out {
		set[n] = struct{}{}
	}
	for n := range g.in {
		set[n] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
