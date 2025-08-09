package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/trinodb/trino-go-client/trino"
)

import "runtime"

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func BenchmarkTrinoServer(b *testing.B) {
	dsn := "http://user@localhost:8080?catalog=tpch&schema=sf1"
	db, err := sql.Open("trino", dsn)
	if err != nil {
		b.Fatalf("Failed to open Trino connection: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1000)
	db.SetMaxIdleConns(100)
	db.SetConnMaxLifetime(10 * time.Minute)

	errorThreshold := 0.9 // 90%

	for concurrency := 10; concurrency <= 100; concurrency += 10 {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			var totalErrors int64
			var allDurations []time.Duration

			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(concurrency)
				durations := make([]time.Duration, concurrency)
				for j := 0; j < concurrency; j++ {
					go func(idx int) {
						defer wg.Done()
						start := time.Now()
						ctx, cancel := context.WithTimeout(context.Background(), 2*60*time.Second)
						defer cancel()
						rows, err := db.QueryContext(ctx, "SELECT * FROM lineitem limit 100")
						if err != nil {
							atomic.AddInt64(&totalErrors, 1)
						} else {
							for rows.Next() {
							}
							if err := rows.Err(); err != nil {
								atomic.AddInt64(&totalErrors, 1)
							}
							rows.Close()
						}
						durations[idx] = time.Since(start)
					}(j)
				}
				wg.Wait()
				allDurations = append(allDurations, durations...)
			}

			totalQueries := len(allDurations)
			var totalDuration time.Duration
			for _, d := range allDurations {
				totalDuration += d
			}
			avgDuration := totalDuration / time.Duration(totalQueries)
			errorRate := float64(totalErrors) / float64(totalQueries)

			b.Logf("Concurrency: %d, Total queries: %d, Total errors: %d, Error rate: %.2f%%, Avg query time: %v",
				concurrency, totalQueries, totalErrors, errorRate*100, avgDuration)

			if errorRate > errorThreshold {
				b.Fatalf("Concurrency: %d, Error rate %.2f%% exceeds threshold %.2f%%, system may be at bottleneck!",
					concurrency, errorRate*100, errorThreshold*100)
			}
		})
	}
}
