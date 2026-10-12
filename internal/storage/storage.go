package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KaavinB/FraudStream/internal/event"
	"github.com/KaavinB/FraudStream/internal/features"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct{ Pool *pgxpool.Pool }
type Score struct {
	ID          string    `json:"transaction_id"`
	Risk        float64   `json:"risk_score"`
	Band        string    `json:"risk_band"`
	Version     string    `json:"model_version"`
	Timestamp   time.Time `json:"scoring_timestamp"`
	InferenceMS float64   `json:"inference_latency_ms"`
}
type Result struct {
	event.Transaction
	AcceptedAt time.Time  `json:"accepted_at"`
	Score      *float64   `json:"risk_score"`
	Decision   *string    `json:"decision"`
	Version    *string    `json:"model_version"`
	Latency    *float64   `json:"precommit_ms"`
	Persisted  *time.Time `json:"persisted_at"`
}

func Open(ctx context.Context, url string) (*DB, error) {
	conf, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	conf.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, conf)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &DB{pool}, nil
}
func (d *DB) Complete(ctx context.Context, id string) (bool, error) {
	var found bool
	err := d.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM decisions WHERE transaction_id=$1)", id).Scan(&found)
	return found, err
}
func (d *DB) Recent(ctx context.Context, id string, limit int) ([]Result, error) {
	// Separate statements: a shared `$1='' OR id=$1` predicate gets a cached generic plan that
	// scans transactions_recent for every single-ID lookup instead of using the primary key.
	const columns = `SELECT t.event,t.accepted_at,d.risk_score,d.decision,d.model_version,d.precommit_ms,d.persisted_at FROM transactions t LEFT JOIN decisions d USING(transaction_id)`
	var rows pgx.Rows
	var err error
	if id == "" {
		rows, err = d.Pool.Query(ctx, columns+` ORDER BY t.accepted_at DESC LIMIT $1`, limit)
	} else {
		rows, err = d.Pool.Query(ctx, columns+` WHERE t.transaction_id=$1`, id)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Result{}
	for rows.Next() {
		var raw []byte
		var r Result
		if err = rows.Scan(&raw, &r.AcceptedAt, &r.Score, &r.Decision, &r.Version, &r.Latency, &r.Persisted); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &r.Transaction); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// Prepare serializes customer AND merchant feature histories across workers.
// The global feature sequencer is a correctness-first bottleneck, explicitly benchmarked.
// Model calls occur after the lock is released, so scoring remains concurrent.
func (d *DB) Prepare(ctx context.Context, store features.Store, envelope event.Envelope, crash func(string)) ([]float64, float64, error) {
	conn, err := d.Pool.Acquire(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, "SELECT pg_advisory_lock(7619231)"); err != nil {
		return nil, 0, err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, e := conn.Exec(c, "SELECT pg_advisory_unlock(7619231)"); e != nil {
			_ = conn.Conn().Close(c)
		}
	}()
	if err = d.reconcile(ctx, conn, &store); err != nil {
		return nil, 0, err
	}
	var original, raw []byte
	var featureSeconds float64
	err = conn.QueryRow(ctx, `SELECT t.event,p.features,p.feature_seconds FROM transactions t JOIN processing_state p USING(transaction_id) WHERE transaction_id=$1`, envelope.Event.ID).Scan(&original, &raw, &featureSeconds)
	if err == nil {
		var previous event.Transaction
		_ = json.Unmarshal(original, &previous)
		a, _ := json.Marshal(previous)
		b, _ := json.Marshal(envelope.Event)
		if string(a) != string(b) {
			return nil, 0, errors.New("CONFLICTING_ID")
		}
		var values []float64
		err = json.Unmarshal(raw, &values)
		return values, featureSeconds, err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, err
	}
	crash("before_features")
	begin := time.Now()
	values, err := store.Snapshot(ctx, envelope.Event)
	featureSeconds = time.Since(begin).Seconds()
	if err != nil {
		return nil, 0, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback(ctx)
	encoded, _ := json.Marshal(envelope.Event)
	featureJSON, _ := json.Marshal(values)
	_, err = tx.Exec(ctx, `INSERT INTO transactions(transaction_id,source_event_id,customer_id,merchant_id,event_timestamp,accepted_at,event,correlation_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, envelope.Event.ID, envelope.Event.SourceID, envelope.Event.Customer, envelope.Event.Merchant, envelope.Event.Timestamp, envelope.AcceptedAt, encoded, envelope.Correlation)
	if err != nil {
		return nil, 0, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO processing_state(transaction_id,features,feature_version,feature_seconds) VALUES($1,$2,$3,$4)`, envelope.Event.ID, featureJSON, features.Version, featureSeconds)
	if err != nil {
		return nil, 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, 0, err
	}
	crash("after_feature_intent")
	if err = store.Apply(ctx, envelope.Event); err != nil {
		return nil, 0, err
	}
	crash("after_features")
	_, err = conn.Exec(ctx, "UPDATE processing_state SET materialized=true,feature_materialized_at=clock_timestamp() WHERE transaction_id=$1", envelope.Event.ID)
	return values, featureSeconds, err
}

func (d *DB) reconcile(ctx context.Context, conn *pgxpool.Conn, store *features.Store) error {
	generation, err := store.Client.Get(ctx, "fraudstream:feature-generation").Result()
	rebuild := false
	if err != nil {
		if err.Error() != "redis: nil" {
			return err
		}
		generation = fmt.Sprintf("%s:gen:%d", store.Namespace, time.Now().UnixNano())
		rebuild = true
	}
	store.Namespace = generation
	rows, err := conn.Query(ctx, `SELECT t.transaction_id,t.event FROM processing_state p JOIN transactions t USING(transaction_id) WHERE $1 OR NOT p.materialized ORDER BY p.sequence`, rebuild)
	if err != nil {
		return err
	}
	// Read in bounded batches via the cursor-backed Rows iterator; PostgreSQL drives recovery order.
	for rows.Next() {
		var id string
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return err
		}
		var e event.Transaction
		if err = json.Unmarshal(raw, &e); err != nil {
			rows.Close()
			return err
		}
		if err = store.Apply(ctx, e); err != nil {
			rows.Close()
			return err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if rebuild {
		if err = store.Client.Set(ctx, "fraudstream:feature-generation", generation, 0).Err(); err != nil {
			return err
		}
	}
	_, err = conn.Exec(ctx, "UPDATE processing_state SET materialized=true,feature_materialized_at=COALESCE(feature_materialized_at,clock_timestamp()) WHERE NOT materialized")
	return err
}

func (d *DB) Persist(ctx context.Context, envelope event.Envelope, score Score, featureSeconds float64) error {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, "INSERT INTO model_versions(model_version) VALUES($1) ON CONFLICT DO NOTHING", score.Version)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO decisions(transaction_id,risk_score,decision,model_version,scoring_timestamp,inference_ms,feature_ms,precommit_ms) VALUES($1,$2,$3,$4,$5,$6,$7,EXTRACT(EPOCH FROM clock_timestamp()-$8::timestamptz)*1000) ON CONFLICT DO NOTHING`, envelope.Event.ID, score.Risk, score.Band, score.Version, score.Timestamp, score.InferenceMS, featureSeconds*1000, envelope.AcceptedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		payload, _ := json.Marshal(map[string]any{"transaction_id": score.ID, "risk_score": score.Risk, "decision": score.Band, "model_version": score.Version, "timestamp": score.Timestamp, "processing_status": "persisted"})
		_, err = tx.Exec(ctx, "INSERT INTO outbox(event_id,topic,message_key,payload) VALUES($1,'transactions.scored',$2,$3)", "score-"+score.ID, envelope.Event.Customer, payload)
		if err != nil {
			return err
		}
		if score.Band != "ALLOW" {
			alertID := "alert-" + score.ID
			payload, _ = json.Marshal(map[string]any{"alert_id": alertID, "transaction_id": score.ID, "risk_score": score.Risk, "decision": score.Band, "model_version": score.Version, "timestamp": score.Timestamp, "processing_status": "persisted", "reason_codes": []string{"MODEL_THRESHOLD_EXCEEDED"}})
			_, err = tx.Exec(ctx, "INSERT INTO fraud_alerts(alert_id,transaction_id,payload) VALUES($1,$2,$3)", alertID, score.ID, payload)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, "INSERT INTO outbox(event_id,topic,message_key,payload) VALUES($1,'fraud.alerts',$2,$3)", alertID, envelope.Event.Customer, payload)
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

func (d *DB) Quarantine(ctx context.Context, topic string, partition int32, offset int64, reason string, payload []byte) error {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Offsets repeat after topic recreation; the payload hash keeps distinct events distinct
	// while redelivery of the same record stays idempotent (see migration 002).
	digest := sha256.Sum256(payload)
	hash := hex.EncodeToString(digest[:])
	_, err = tx.Exec(ctx, `INSERT INTO quarantined_events(topic,partition,event_offset,reason,payload,payload_sha256) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, topic, partition, offset, reason, string(payload), hash)
	if err != nil {
		return err
	}
	id := fmt.Sprintf("dlq-%s-%d-%d-%s", topic, partition, offset, hash[:16])
	raw, _ := json.Marshal(map[string]any{"topic": topic, "partition": partition, "offset": offset, "reason": reason, "raw_payload": string(payload)})
	_, err = tx.Exec(ctx, "INSERT INTO outbox(event_id,topic,message_key,payload) VALUES($1,'transactions.dead-letter',$1,$2) ON CONFLICT DO NOTHING", id, raw)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
