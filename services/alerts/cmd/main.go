package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/KaavinB/FraudStream/internal/config"
	"github.com/KaavinB/FraudStream/internal/kafka"
	"github.com/KaavinB/FraudStream/internal/storage"
)

func publish(ctx context.Context, db *storage.DB) error {
	client, err := kafka.Producer(config.Get("KAFKA_BROKERS", "localhost:9092"))
	if err != nil {
		return err
	}
	defer client.Close()
	for ctx.Err() == nil {
		tx, err := db.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		var id, topic, key string
		var raw []byte
		err = tx.QueryRow(ctx, "SELECT event_id,topic,message_key,payload FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED").Scan(&id, &topic, &key, &raw)
		if err != nil {
			_ = tx.Rollback(ctx)
			if err.Error() != "no rows in result set" {
				return err
			}
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			}
			continue
		}
		attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = kafka.Publish(attempt, client, topic, key, raw)
		cancel()
		if err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if config.Get("CRASH_AT", "") == "during_alert_publication" && topic == "fraud.alerts" && (config.Get("CRASH_TRANSACTION_ID", "") == "" || id == "alert-"+config.Get("CRASH_TRANSACTION_ID", "")) {
			os.Exit(86)
		}
		_, err = tx.Exec(ctx, "UPDATE outbox SET published_at=clock_timestamp(),attempts=attempts+1 WHERE event_id=$1", id)
		if err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
	}
	return ctx.Err()
}
func consume(ctx context.Context, db *storage.DB) error {
	client, err := kafka.Consumer(config.Get("KAFKA_BROKERS", "localhost:9092"), "fraudstream-alerts", "fraud.alerts", time.Duration(config.Int("KAFKA_SESSION_TIMEOUT_MS", 6000))*time.Millisecond)
	if err != nil {
		return err
	}
	defer client.Close()
	for ctx.Err() == nil {
		fetched := client.PollRecords(ctx, 256)
		if err = fetched.Err(); err != nil {
			return err
		}
		for _, record := range fetched.Records() {
			var payload struct {
				ID string `json:"alert_id"`
			}
			if err = json.Unmarshal(record.Value, &payload); err != nil {
				return err
			}
			if payload.ID == "" {
				return &json.SyntaxError{}
			}
			if _, err = db.Pool.Exec(ctx, "UPDATE fraud_alerts SET observed_at=COALESCE(observed_at,clock_timestamp()) WHERE alert_id=$1", payload.ID); err != nil {
				return err
			}
			if err = client.CommitRecords(ctx, record); err != nil {
				return err
			}
		}
		client.AllowRebalance()
	}
	return ctx.Err()
}
func main() {
	ctx, cancel := config.Context()
	defer cancel()
	db, err := storage.Open(ctx, config.Get("DATABASE_URL", ""))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Pool.Close()
	failures := make(chan error, 2)
	go func() { failures <- publish(ctx, db) }()
	go func() { failures <- consume(ctx, db) }()
	err = <-failures
	cancel()
	if err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
