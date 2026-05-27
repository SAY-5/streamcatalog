package catalog

import (
	"context"
	"errors"
	"testing"
)

// A subscribe call records both a subscription and a consumer lineage edge, and
// the lineage view of the stream then reports an upstream producer and a
// downstream consumer.
func TestSubscribeProvisionsAccessAndLineage(t *testing.T) {
	svc := newTestService()
	ctx := context.Background()

	st, err := svc.Register(ctx, sampleInput("ledger"), "team-pay")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	res, err := svc.Subscribe(ctx, st.ID, "team-analytics", "analytics")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if res.Subscription.Consumer != "team-analytics" {
		t.Fatalf("subscription consumer = %q", res.Subscription.Consumer)
	}
	if res.Edge.Kind != EdgeConsume {
		t.Fatalf("edge kind = %q, want consume", res.Edge.Kind)
	}

	ok, err := svc.CanRead(ctx, st.ID, "team-analytics", "analytics")
	if err != nil {
		t.Fatalf("can read: %v", err)
	}
	if !ok {
		t.Fatal("subscribed consumer should be able to read")
	}

	view, err := svc.Lineage(ctx, st.ID)
	if err != nil {
		t.Fatalf("lineage: %v", err)
	}
	if len(view.Upstream) != 1 || view.Upstream[0] != "team:team-pay" {
		t.Fatalf("upstream = %v, want [team:team-pay]", view.Upstream)
	}
	if len(view.Downstream) != 1 || view.Downstream[0] != "team:team-analytics" {
		t.Fatalf("downstream = %v, want [team:team-analytics]", view.Downstream)
	}
}

func TestUpdateSchemaAcceptsCompatibleRejectsIncompatible(t *testing.T) {
	svc := newTestService()
	ctx := context.Background()

	st, err := svc.Register(ctx, sampleInput("profile"), "team-pay")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	compatible := `{"fields":{"id":{"type":"string"},"email":{"type":"string","optional":true}}}`
	updated, err := svc.UpdateSchema(ctx, st.ID, compatible)
	if err != nil {
		t.Fatalf("compatible update rejected: %v", err)
	}
	if updated.SchemaVersion != 2 {
		t.Fatalf("schema version = %d, want 2", updated.SchemaVersion)
	}

	incompatible := `{"fields":{"id":{"type":"int"}}}`
	if _, updErr := svc.UpdateSchema(ctx, st.ID, incompatible); !errors.Is(updErr, ErrSchemaConflict) {
		t.Fatalf("err = %v, want ErrSchemaConflict", updErr)
	}
}

func TestAddDerivationLinksStreams(t *testing.T) {
	svc := newTestService()
	ctx := context.Background()

	src, err := svc.Register(ctx, sampleInput("raw"), "team-pay")
	if err != nil {
		t.Fatalf("register src: %v", err)
	}
	dst, err := svc.Register(ctx, sampleInput("aggregated"), "team-pay")
	if err != nil {
		t.Fatalf("register dst: %v", err)
	}

	if derErr := svc.AddDerivation(ctx, src.ID, dst.ID); derErr != nil {
		t.Fatalf("add derivation: %v", derErr)
	}

	view, err := svc.Lineage(ctx, src.ID)
	if err != nil {
		t.Fatalf("lineage: %v", err)
	}
	found := false
	for _, n := range view.Downstream {
		if n == "stream:"+dst.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("derived stream not downstream: %v", view.Downstream)
	}
}
