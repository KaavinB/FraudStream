package ingestion

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/KaavinB/FraudStream/internal/event"
	"github.com/KaavinB/FraudStream/internal/storage"
	"github.com/KaavinB/FraudStream/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Publisher interface {
	Publish(context.Context, event.Envelope) error
	Ready(context.Context) error
}
type Reader interface {
	Recent(context.Context, string, int) ([]storage.Result, error)
}
type API struct {
	Publisher Publisher
	DB        Reader
	APIKey    string
	Slots     chan struct{}
	Rate      int
	mutex     sync.Mutex
	second    int64
	used      int
	Extra     http.Handler
}

func JSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { JSON(w, 200, map[string]string{"status": "alive"}) })
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := a.Publisher.Ready(ctx); err != nil {
			JSON(w, 503, map[string]string{"status": "unavailable"})
			return
		}
		JSON(w, 200, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("POST /api/v1/transactions", a.post)
	mux.HandleFunc("GET /api/v1/transactions", a.get)
	mux.HandleFunc("GET /api/v1/transactions/{id}", a.get)
	mux.HandleFunc("GET /api/v1/transactions/{id}/decision", a.get)
	if a.Extra != nil {
		mux.Handle("/api/v1/analytics/", a.Extra)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+a.APIKey)) != 1 {
			JSON(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		mux.ServeHTTP(w, r)
	})
}
func (a *API) allow() bool {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	now := time.Now().Unix()
	if now != a.second {
		a.second = now
		a.used = 0
	}
	a.used++
	return a.used <= a.Rate
}
func (a *API) post(w http.ResponseWriter, r *http.Request) {
	if !a.allow() {
		JSON(w, 429, map[string]string{"error": "rate limit exceeded"})
		return
	}
	select {
	case a.Slots <- struct{}{}:
		defer func() { <-a.Slots }()
	default:
		JSON(w, 503, map[string]string{"error": "ingestion capacity exceeded"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16384)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var tx event.Transaction
	if err := decoder.Decode(&tx); err != nil {
		JSON(w, 400, map[string]string{"error": "invalid transaction JSON"})
		return
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		JSON(w, 400, map[string]string{"error": "one JSON object required"})
		return
	}
	if err := tx.Validate(); err != nil {
		JSON(w, 422, map[string]string{"error": err.Error()})
		return
	}
	tx.Timestamp = tx.Timestamp.UTC()
	accepted := time.Now().UTC()
	e := event.Envelope{Event: tx, AcceptedAt: accepted, Correlation: fmt.Sprintf("%s-%d", tx.ID, accepted.UnixNano())}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	begin := time.Now()
	if err := a.Publisher.Publish(ctx, e); err != nil {
		telemetry.Events.WithLabelValues("ingestion", "error").Inc()
		JSON(w, 503, map[string]string{"error": "Kafka publish unconfirmed; retry with same transaction ID"})
		return
	}
	telemetry.Duration.WithLabelValues("publish").Observe(time.Since(begin).Seconds())
	telemetry.Events.WithLabelValues("ingestion", "accepted").Inc()
	w.Header().Set("Location", "/api/v1/transactions/"+tx.ID+"/decision")
	JSON(w, 202, map[string]any{"transaction_id": tx.ID, "status": "pending", "accepted_at": accepted, "correlation_id": e.Correlation})
}
func (a *API) get(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	id := r.PathValue("id")
	rows, err := a.DB.Recent(ctx, id, 100)
	if err != nil {
		JSON(w, 503, map[string]string{"error": "decision store unavailable"})
		return
	}
	if id == "" {
		JSON(w, 200, rows)
		return
	}
	if len(rows) == 0 {
		JSON(w, 404, map[string]string{"error": "transaction not materialized; may still be queued"})
		return
	}
	if strings.HasSuffix(r.URL.Path, "/decision") && rows[0].Decision == nil {
		JSON(w, 202, map[string]string{"status": "pending"})
		return
	}
	JSON(w, 200, rows[0])
}
