package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	_ "github.com/trinodb/trino-go-client/trino"
)

func TestTrinoProtocol(t *testing.T) {
	start := time.Now()
	dsn := "http://user@localhost:8080?catalog=tpch&schema=sf1"
	db, err := sql.Open("trino", dsn)
	if err != nil {
		log.Fatalf("Failed to open Trino connection: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "SELECT * FROM lineitem limit 100")
	//rows, err := db.QueryContext(ctx, "SELECT 1")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Rows error: %v", err)
	}
	elapsed := time.Since(start)
	fmt.Printf("signal query elapsed %s, rows:%d\n", elapsed.Round(time.Millisecond), count)
}
