package event

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func valid() Transaction {
	return Transaction{SchemaVersion: 1, ID: "tx", SourceID: "tx", Customer: "c", Merchant: "m", Amount: 12.5, Currency: "USD", Type: "PAYMENT", Timestamp: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
}
func TestValidation(t *testing.T) {
	if err := valid().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Transaction){func(e *Transaction) { e.ID = "bad space" }, func(e *Transaction) { e.Amount = math.NaN() }, func(e *Transaction) { e.Amount = -1 }, func(e *Transaction) { e.Amount = 1.001 }, func(e *Transaction) { e.SchemaVersion = 2 }, func(e *Transaction) { e.Timestamp = time.Time{} }} {
		e := valid()
		mutate(&e)
		if e.Validate() == nil {
			t.Fatal("invalid event accepted")
		}
	}
}
func TestSerialization(t *testing.T) {
	e := Envelope{Event: valid(), AcceptedAt: time.Now().UTC(), Correlation: "trace"}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(raw)
	if err != nil || decoded.Event.ID != e.Event.ID {
		t.Fatal(err)
	}
}
func BenchmarkValidation(b *testing.B) {
	e := valid()
	b.ReportAllocs()
	for b.Loop() {
		_ = e.Validate()
	}
}
func BenchmarkSerialization(b *testing.B) {
	e := Envelope{Event: valid(), AcceptedAt: time.Now()}
	b.ReportAllocs()
	for b.Loop() {
		_, _ = json.Marshal(e)
	}
}
