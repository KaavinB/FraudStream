package ingestion

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KaavinB/FraudStream/internal/event"
	"github.com/KaavinB/FraudStream/internal/storage"
)

type fake struct {
	err   error
	count int
}

func (p *fake) Publish(ctx context.Context, e event.Envelope) error { p.count++; return p.err }
func (p *fake) Ready(ctx context.Context) error                     { return p.err }
func (p *fake) Recent(ctx context.Context, id string, n int) ([]storage.Result, error) {
	return []storage.Result{}, nil
}

const body = `{"schema_version":1,"transaction_id":"tx1","source_event_id":"tx1","customer_id":"c1","merchant_id":"m1","amount":12.5,"currency":"USD","transaction_type":"PAYMENT","timestamp":"2025-01-01T00:00:00Z"}`

func TestAPI(t *testing.T) {
	for _, test := range []struct {
		name, input, token string
		err                error
		want               int
	}{{"accepted", body, "secret", nil, 202}, {"unauthorized", body, "bad", nil, 401}, {"invalid", "{}", "secret", nil, 422}, {"publish failure", body, "secret", errors.New("broker down"), 503}, {"labels", strings.Replace(body, `"amount":12.5`, `"fraud":1,"amount":12.5`, 1), "secret", nil, 400}, {"multiple JSON", body + body, "secret", nil, 400}} {
		t.Run(test.name, func(t *testing.T) {
			p := &fake{err: test.err}
			api := API{Publisher: p, DB: p, APIKey: "secret", Slots: make(chan struct{}, 2), Rate: 100}
			r := httptest.NewRequest("POST", "/api/v1/transactions", strings.NewReader(test.input))
			r.Header.Set("Authorization", "Bearer "+test.token)
			w := httptest.NewRecorder()
			api.Handler().ServeHTTP(w, r)
			if w.Code != test.want {
				t.Fatalf("got %d: %s", w.Code, w.Body.String())
			}
		})
	}
}
func BenchmarkHTTPAcceptanceMockPublisher(b *testing.B) {
	p := &fake{}
	api := API{Publisher: p, DB: p, APIKey: "secret", Slots: make(chan struct{}, 2), Rate: 1000000000}
	h := api.Handler()
	b.ReportAllocs()
	for b.Loop() {
		r := httptest.NewRequest("POST", "/api/v1/transactions", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer secret")
		h.ServeHTTP(httptest.NewRecorder(), r)
	}
}
