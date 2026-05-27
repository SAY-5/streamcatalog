package schema

import "testing"

func TestCompatible(t *testing.T) {
	base := `{"fields":{"id":{"type":"string"},"amount":{"type":"int"}}}`
	tests := []struct {
		name    string
		oldDef  string
		newDef  string
		wantOK  bool
		wantLen int
	}{
		{
			name:   "add optional field is compatible",
			oldDef: base,
			newDef: `{"fields":{"id":{"type":"string"},"amount":{"type":"int"},"note":{"type":"string","optional":true}}}`,
			wantOK: true,
		},
		{
			name:    "add required field is incompatible",
			oldDef:  base,
			newDef:  `{"fields":{"id":{"type":"string"},"amount":{"type":"int"},"note":{"type":"string"}}}`,
			wantOK:  false,
			wantLen: 1,
		},
		{
			name:    "remove field is incompatible",
			oldDef:  base,
			newDef:  `{"fields":{"id":{"type":"string"}}}`,
			wantOK:  false,
			wantLen: 1,
		},
		{
			name:    "change type is incompatible",
			oldDef:  base,
			newDef:  `{"fields":{"id":{"type":"string"},"amount":{"type":"string"}}}`,
			wantOK:  false,
			wantLen: 1,
		},
		{
			name:   "identical schema is compatible",
			oldDef: base,
			newDef: base,
			wantOK: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ok, issues, err := Compatible(tc.oldDef, tc.newDef)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (issues: %v)", ok, tc.wantOK, issues)
			}
			if !tc.wantOK && len(issues) != tc.wantLen {
				t.Fatalf("got %d issues, want %d: %v", len(issues), tc.wantLen, issues)
			}
		})
	}
}

func TestParseRejectsBadJSON(t *testing.T) {
	if _, err := Parse("{not json"); err == nil {
		t.Fatal("expected error parsing invalid JSON")
	}
}
