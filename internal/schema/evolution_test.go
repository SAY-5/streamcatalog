package schema

import "testing"

// A chain of compatible evolutions must remain pairwise compatible: adding
// optional fields across several versions never breaks compatibility.
func TestCompatibleEvolutionChain(t *testing.T) {
	v1 := `{"fields":{"id":{"type":"string"}}}`
	v2 := `{"fields":{"id":{"type":"string"},"name":{"type":"string","optional":true}}}`
	v3 := `{"fields":{"id":{"type":"string"},"name":{"type":"string","optional":true},"age":{"type":"int","optional":true}}}`

	for _, step := range [][2]string{{v1, v2}, {v2, v3}, {v1, v3}} {
		ok, issues, err := Compatible(step[0], step[1])
		if err != nil {
			t.Fatalf("compatible: %v", err)
		}
		if !ok {
			t.Fatalf("expected compatible evolution, got issues: %v", issues)
		}
	}
}

// An evolution that drops a field added earlier in the chain is rejected.
func TestEvolutionRejectsFieldRemoval(t *testing.T) {
	v2 := `{"fields":{"id":{"type":"string"},"name":{"type":"string","optional":true}}}`
	v3 := `{"fields":{"name":{"type":"string","optional":true}}}`

	ok, issues, err := Compatible(v2, v3)
	if err != nil {
		t.Fatalf("compatible: %v", err)
	}
	if ok {
		t.Fatal("dropping the id field should be rejected")
	}
	if len(issues) != 1 || issues[0].Field != "id" {
		t.Fatalf("issues = %v, want a single id removal", issues)
	}
}
