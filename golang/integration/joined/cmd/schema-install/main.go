package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	namespace := postgresstore.Namespace{Database: "llmtw_worker", Schema: "llmtw_state", TablePrefix: "llmtw_"}
	if err = postgresstore.Install(ctx, pool, namespace); err != nil {
		log.Fatal(err)
	}
	if err = postgresstore.Verify(ctx, pool, namespace); err != nil {
		log.Fatal(err)
	}
	log.Print("JOINED_SMOKE_SCHEMA_READY")
}
