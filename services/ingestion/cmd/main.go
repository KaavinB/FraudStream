package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/KaavinB/FraudStream/internal/analytics"
	"github.com/KaavinB/FraudStream/internal/config"
	"github.com/KaavinB/FraudStream/internal/event"
	"github.com/KaavinB/FraudStream/internal/ingestion"
	"github.com/KaavinB/FraudStream/internal/kafka"
	"github.com/KaavinB/FraudStream/internal/storage"
	"github.com/twmb/franz-go/pkg/kgo"
)

type producer struct{ client *kgo.Client }

func (p producer) Publish(ctx context.Context, e event.Envelope) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return kafka.Publish(ctx, p.client, "transactions.raw", e.Event.Customer, raw)
}
func (p producer) Ready(ctx context.Context) error { return p.client.Ping(ctx) }
func main() {
	ctx, cancel := config.Context()
	defer cancel()
	key := config.Get("API_KEY", "")
	if len(key) < 16 {
		log.Fatal("API_KEY must have at least 16 characters")
	}
	db, err := storage.Open(ctx, config.Get("DATABASE_URL", ""))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Pool.Close()
	client, err := kafka.Producer(config.Get("KAFKA_BROKERS", "localhost:9092"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	api := &ingestion.API{Publisher: producer{client}, DB: db, APIKey: key, Slots: make(chan struct{}, config.Int("INGESTION_CONCURRENCY", 128)), Rate: config.Int("RATE_LIMIT", 10000), Extra: analytics.Handler(db)}
	config.Serve(ctx, config.Get("HTTP_ADDR", ":8080"), api.Handler())
}
