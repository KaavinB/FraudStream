package main

import (
	"log"
	"net/http"
	"time"

	"github.com/KaavinB/FraudStream/internal/config"
	"github.com/KaavinB/FraudStream/internal/features"
	"github.com/KaavinB/FraudStream/internal/kafka"
	"github.com/KaavinB/FraudStream/internal/processor"
	"github.com/KaavinB/FraudStream/internal/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, cancel := config.Context()
	defer cancel()
	db, err := storage.Open(ctx, config.Get("DATABASE_URL", ""))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Pool.Close()
	options, err := redis.ParseURL(config.Get("REDIS_URL", "redis://localhost:6379"))
	if err != nil {
		log.Fatal(err)
	}
	options.ReadTimeout = 2 * time.Second
	options.WriteTimeout = 2 * time.Second
	cache := redis.NewClient(options)
	defer cache.Close()
	client, err := kafka.Consumer(config.Get("KAFKA_BROKERS", "localhost:9092"), "fraudstream-workers", "transactions.raw", time.Duration(config.Int("KAFKA_SESSION_TIMEOUT_MS", 6000))*time.Millisecond)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	go config.Serve(ctx, ":8081", mux)
	worker := processor.Worker{Client: client, DB: db, Features: features.Store{Client: cache, Namespace: "fraudstream:prior-v1"}, HTTP: &http.Client{Timeout: 3 * time.Second}, InferenceURL: config.Get("INFERENCE_URL", "http://localhost:8001"), Concurrency: config.Int("WORKER_CONCURRENCY", 4), CrashAt: config.Get("CRASH_AT", ""), CrashID: config.Get("CRASH_TRANSACTION_ID", "")}
	if err = worker.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
