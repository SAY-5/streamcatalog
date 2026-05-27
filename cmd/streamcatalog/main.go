// Command streamcatalog runs the catalog HTTP service.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SAY-5/streamcatalog/internal/api"
	"github.com/SAY-5/streamcatalog/internal/catalog"
	"github.com/SAY-5/streamcatalog/internal/kafkax"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	dsn := env("DATABASE_URL", "postgres://catalog:catalog@localhost:5432/catalog?sslmode=disable")
	broker := env("KAFKA_BROKER", "localhost:9092")
	addr := env("LISTEN_ADDR", ":8080")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	store := catalog.NewStore(pool)
	migCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if migErr := store.Migrate(migCtx); migErr != nil {
		log.Fatalf("migrate: %v", migErr)
	}

	svc := catalog.NewService(store, kafkax.New(broker))
	server := api.NewServer(svc)

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           server.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("streamcatalog listening on %s", addr)
	if listenErr := httpSrv.ListenAndServe(); listenErr != nil {
		log.Fatalf("serve: %v", listenErr)
	}
}
