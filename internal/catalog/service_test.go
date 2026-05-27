package catalog

import (
	"context"
	"errors"
	"testing"
)

func newTestService() *Service {
	return NewService(newMemStore(), newFakeKafka())
}

func sampleInput(name string) RegisterInput {
	return RegisterInput{
		Name:        name,
		Topic:       "topic." + name,
		Domain:      "payments",
		Owner:       "team-pay",
		Tags:        []string{"billing"},
		RetentionMS: 604800000,
		Access:      AccessPublic,
		SchemaType:  SchemaJSON,
		SchemaDef:   `{"fields":{"id":{"type":"string"}}}`,
	}
}

func TestRegisterAndDiscover(t *testing.T) {
	svc := newTestService()
	ctx := context.Background()

	st, err := svc.Register(ctx, sampleInput("orders"), "team-pay")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if st.SchemaVersion != 1 {
		t.Fatalf("schema version = %d, want 1", st.SchemaVersion)
	}

	found, err := svc.Search(ctx, SearchFilter{Domain: "payments"})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(found) != 1 || found[0].ID != st.ID {
		t.Fatalf("discovery did not return the registered stream: %v", found)
	}
}

func TestRegisterRejectsInvalidInput(t *testing.T) {
	svc := newTestService()
	in := sampleInput("bad")
	in.Owner = ""
	if _, err := svc.Register(context.Background(), in, "team-pay"); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("err = %v, want ErrInvalidInput", err)
	}
}

func TestSubscribeEnforcesAccessModel(t *testing.T) {
	ctx := context.Background()

	t.Run("private rejects non allow-listed consumer", func(t *testing.T) {
		svc := newTestService()
		in := sampleInput("private-stream")
		in.Access = AccessPrivate
		in.AllowList = []string{"team-allowed"}
		st, err := svc.Register(ctx, in, "team-pay")
		if err != nil {
			t.Fatalf("register: %v", err)
		}
		if _, subErr := svc.Subscribe(ctx, st.ID, "team-other", "marketing"); !errors.Is(subErr, ErrAccessDenied) {
			t.Fatalf("err = %v, want ErrAccessDenied", subErr)
		}
	})

	t.Run("public allows any consumer", func(t *testing.T) {
		svc := newTestService()
		st, err := svc.Register(ctx, sampleInput("public-stream"), "team-pay")
		if err != nil {
			t.Fatalf("register: %v", err)
		}
		if _, subErr := svc.Subscribe(ctx, st.ID, "team-any", "marketing"); subErr != nil {
			t.Fatalf("subscribe: %v", subErr)
		}
	})
}
