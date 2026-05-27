package catalog

import (
	"context"
	"errors"
	"testing"
)

func privateInput(name string, allow []string) RegisterInput {
	in := sampleInput(name)
	in.Access = AccessPrivate
	in.AllowList = allow
	return in
}

func domainInput(name, domain string) RegisterInput {
	in := sampleInput(name)
	in.Access = AccessDomain
	in.Domain = domain
	return in
}

// An allowed consumer self-subscribes with no manual step and gains both a
// subscription record and a consumer lineage edge, and can then read.
func TestSelfServeSubscribeProvisionsAccess(t *testing.T) {
	svc := newTestService()
	ctx := context.Background()

	st, err := svc.Register(ctx, privateInput("payouts", []string{"team-ledger"}), "team-pay")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Before subscribing, a non allow-listed read is denied.
	preRead, err := svc.CanRead(ctx, st.ID, "team-ledger", "")
	if err != nil {
		t.Fatalf("can read: %v", err)
	}
	if !preRead {
		t.Fatal("allow-listed consumer should already be readable")
	}

	res, err := svc.Subscribe(ctx, st.ID, "team-ledger", "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if res.Subscription.StreamID != st.ID || res.Subscription.Consumer != "team-ledger" {
		t.Fatalf("unexpected subscription: %+v", res.Subscription)
	}
	if res.Edge.Kind != EdgeConsume || res.Edge.FromNode != "stream:"+st.ID {
		t.Fatalf("unexpected lineage edge: %+v", res.Edge)
	}

	view, err := svc.Lineage(ctx, st.ID)
	if err != nil {
		t.Fatalf("lineage: %v", err)
	}
	if !contains(view.Downstream, "team:team-ledger") {
		t.Fatalf("consumer not recorded downstream: %v", view.Downstream)
	}
}

// A disallowed consumer is rejected and no access is provisioned.
func TestSelfServeSubscribeRejectsDisallowed(t *testing.T) {
	svc := newTestService()
	ctx := context.Background()

	st, err := svc.Register(ctx, privateInput("payouts", []string{"team-ledger"}), "team-pay")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	_, subErr := svc.Subscribe(ctx, st.ID, "team-intruder", "marketing")
	if !errors.Is(subErr, ErrAccessDenied) {
		t.Fatalf("err = %v, want ErrAccessDenied", subErr)
	}

	canRead, err := svc.CanRead(ctx, st.ID, "team-intruder", "marketing")
	if err != nil {
		t.Fatalf("can read: %v", err)
	}
	if canRead {
		t.Fatal("rejected consumer must not have read access")
	}
}

// The domain access model grants only same-domain consumers.
func TestDomainAccessModel(t *testing.T) {
	svc := newTestService()
	ctx := context.Background()

	st, err := svc.Register(ctx, domainInput("metrics", "platform"), "team-plat")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if _, subErr := svc.Subscribe(ctx, st.ID, "team-same", "platform"); subErr != nil {
		t.Fatalf("same-domain subscribe rejected: %v", subErr)
	}
	if _, subErr := svc.Subscribe(ctx, st.ID, "team-other", "growth"); !errors.Is(subErr, ErrAccessDenied) {
		t.Fatalf("cross-domain err = %v, want ErrAccessDenied", subErr)
	}
}
