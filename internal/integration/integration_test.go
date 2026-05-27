//go:build integration

// Package integration exercises the catalog against real Postgres and Kafka
// containers started with Testcontainers.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/SAY-5/streamcatalog/internal/catalog"
	"github.com/SAY-5/streamcatalog/internal/kafkax"
)

func startPostgres(ctx context.Context, t *testing.T) *pgxpool.Pool {
	t.Helper()
	container, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("catalog"),
		tcpostgres.WithUsername("catalog"),
		tcpostgres.WithPassword("catalog"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func startKafka(ctx context.Context, t *testing.T) string {
	t.Helper()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.0",
		tckafka.WithClusterID("catalog-test"),
	)
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	brokers, err := container.Brokers(ctx)
	if err != nil || len(brokers) == 0 {
		t.Fatalf("brokers: %v", err)
	}
	return brokers[0]
}

func TestRegisterDiscoverSubscribeAgainstRealServices(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	pool := startPostgres(ctx, t)
	broker := startKafka(ctx, t)

	store := catalog.NewStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	svc := catalog.NewService(store, kafkax.New(broker))

	in := catalog.RegisterInput{
		Name:        "orders.v1",
		Topic:       "orders-v1",
		Domain:      "commerce",
		Owner:       "team-orders",
		Tags:        []string{"orders", "commerce"},
		RetentionMS: 604800000,
		Access:      catalog.AccessPublic,
		SchemaType:  catalog.SchemaJSON,
		SchemaDef:   `{"fields":{"id":{"type":"string"}}}`,
	}

	st, err := svc.Register(ctx, in, "team-orders")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	found, err := svc.Search(ctx, catalog.SearchFilter{Tag: "orders"})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(found) != 1 || found[0].ID != st.ID {
		t.Fatalf("discovery returned %v", found)
	}

	if _, subErr := svc.Subscribe(ctx, st.ID, "team-analytics", "analytics"); subErr != nil {
		t.Fatalf("subscribe: %v", subErr)
	}

	view, err := svc.Lineage(ctx, st.ID)
	if err != nil {
		t.Fatalf("lineage: %v", err)
	}
	if len(view.Upstream) == 0 || len(view.Downstream) == 0 {
		t.Fatalf("expected upstream and downstream nodes, got %+v", view)
	}
}
