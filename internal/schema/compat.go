// Package schema implements a schema-evolution compatibility check used when a
// stream's schema is updated to a new version.
//
// Compatibility rule (full transitive compatibility):
//
//	A new schema version is accepted only if it is both backward and forward
//	compatible with the previous version. For the JSON object schemas handled
//	here that means:
//	  - No existing field may be removed (a removed field breaks readers that
//	    still expect it: forward incompatibility).
//	  - No existing field may change type (old and new readers disagree on the
//	    wire representation).
//	  - A newly added field is only allowed when it is optional, because a new
//	    required field cannot be satisfied by existing producers (backward
//	    incompatibility).
//
// Schemas are described as a flat map of field name to a field descriptor so
// the rule is explicit and testable without pulling in a full Avro/JSON-schema
// resolver.
package schema

import (
	"encoding/json"
	"fmt"
)

// Field describes a single field in a stream schema.
type Field struct {
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
}

// Schema is a flat set of named fields.
type Schema struct {
	Fields map[string]Field `json:"fields"`
}

// Parse decodes a schema definition from its JSON form.
func Parse(def string) (Schema, error) {
	var s Schema
	if err := json.Unmarshal([]byte(def), &s); err != nil {
		return Schema{}, fmt.Errorf("parse schema: %w", err)
	}
	if s.Fields == nil {
		s.Fields = map[string]Field{}
	}
	return s, nil
}

// Incompatibility explains why two schema versions are not compatible.
type Incompatibility struct {
	Field  string
	Reason string
}

func (i Incompatibility) String() string {
	return fmt.Sprintf("field %q: %s", i.Field, i.Reason)
}

// Check returns the list of incompatibilities between an old and new schema.
// An empty result means the new schema is a compatible evolution.
func Check(oldS, newS Schema) []Incompatibility {
	var out []Incompatibility
	for name, oldField := range oldS.Fields {
		newField, present := newS.Fields[name]
		if !present {
			out = append(out, Incompatibility{Field: name, Reason: "removed existing field"})
			continue
		}
		if newField.Type != oldField.Type {
			out = append(out, Incompatibility{
				Field:  name,
				Reason: fmt.Sprintf("type changed from %s to %s", oldField.Type, newField.Type),
			})
		}
	}
	for name, newField := range newS.Fields {
		if _, present := oldS.Fields[name]; present {
			continue
		}
		if !newField.Optional {
			out = append(out, Incompatibility{Field: name, Reason: "added required field"})
		}
	}
	return out
}

// Compatible reports whether newDef is a compatible evolution of oldDef.
func Compatible(oldDef, newDef string) (bool, []Incompatibility, error) {
	oldS, err := Parse(oldDef)
	if err != nil {
		return false, nil, err
	}
	newS, err := Parse(newDef)
	if err != nil {
		return false, nil, err
	}
	issues := Check(oldS, newS)
	return len(issues) == 0, issues, nil
}
