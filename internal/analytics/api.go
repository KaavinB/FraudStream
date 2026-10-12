package analytics

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/KaavinB/FraudStream/internal/config"
	"github.com/KaavinB/FraudStream/internal/storage"
)

func respond(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func Handler(db *storage.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/analytics/overview", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		var total, alerts, pending, failures int64
		var p95, mean *float64
		var rate float64
		var model *string
		err := db.Pool.QueryRow(ctx, `SELECT count(*),percentile_cont(.95) WITHIN GROUP(ORDER BY precommit_ms),avg(risk_score),(SELECT count(*) FROM fraud_alerts),(SELECT count(*) FROM processing_state p WHERE NOT EXISTS(SELECT 1 FROM decisions d WHERE d.transaction_id=p.transaction_id)),(SELECT count(*) FROM quarantined_events),(SELECT count(*)/60.0 FROM decisions WHERE persisted_at>clock_timestamp()-interval '60 seconds'),(SELECT model_version FROM decisions ORDER BY persisted_at DESC LIMIT 1) FROM decisions`).Scan(&total, &p95, &mean, &alerts, &pending, &failures, &rate, &model)
		if err != nil {
			respond(w, 503, map[string]string{"error": "store unavailable"})
			return
		}
		respond(w, 200, map[string]any{"completed_unique_decisions": total, "precommit_p95_ms": p95, "mean_risk_score": mean, "logical_alerts": alerts, "pending_scoring": pending, "quarantined_events": failures, "completed_per_second_last_60s": rate, "active_model": model, "consumer_lag": nil, "ingested_transactions": nil, "note": "precommit_ms spans API acceptance to the decision INSERT (includes Kafka queueing, excludes COMMIT); use worker end_to_end metrics and benchmark post-ack observation for durable completion. Null means unavailable."})
	})
	mux.HandleFunc("GET /api/v1/analytics/alerts", func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.Pool.Query(r.Context(), "SELECT payload,observed_at FROM fraud_alerts ORDER BY created_at DESC LIMIT 100")
		if err != nil {
			respond(w, 503, map[string]string{"error": "store unavailable"})
			return
		}
		defer rows.Close()
		out := []any{}
		for rows.Next() {
			var raw []byte
			var observed *time.Time
			if err = rows.Scan(&raw, &observed); err != nil {
				respond(w, 503, map[string]string{"error": "read failed"})
				return
			}
			var v map[string]any
			if err = json.Unmarshal(raw, &v); err != nil {
				respond(w, 503, map[string]string{"error": "decode failed"})
				return
			}
			v["observed_at"] = observed
			out = append(out, v)
		}
		if rows.Err() != nil {
			respond(w, 503, map[string]string{"error": "read failed"})
			return
		}
		respond(w, 200, out)
	})
	// Absolute defaults match the compose mounts; the distroless image's working directory is
	// /home/nonroot, so relative paths never found the mounted reports.
	reports := []struct{ path, file string }{
		{"model", config.Get("MODEL_REPORT_PATH", "/ml/artifacts/model-info.json")},
		{"benchmarks", config.Get("BENCHMARK_REPORT_PATH", "/benchmarks/results/report.json")},
	}
	for _, entry := range reports {
		mux.HandleFunc("GET /api/v1/analytics/"+entry.path, func(w http.ResponseWriter, r *http.Request) {
			raw, err := os.ReadFile(entry.file)
			if err != nil {
				respond(w, 404, map[string]string{"status": "no measured report available"})
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(raw)
		})
	}
	return mux
}
