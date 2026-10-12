package event

import (
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"time"
)

var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,96}$`)

type Transaction struct {
	SchemaVersion int       `json:"schema_version"`
	ID            string    `json:"transaction_id"`
	SourceID      string    `json:"source_event_id"`
	Customer      string    `json:"customer_id"`
	Merchant      string    `json:"merchant_id"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	Type          string    `json:"transaction_type"`
	Timestamp     time.Time `json:"timestamp"`
	Device        string    `json:"device_id,omitempty"`
}

func (t Transaction) Validate() error {
	if t.SchemaVersion != 1 {
		return errors.New("unsupported schema_version")
	}
	for _, id := range []string{t.ID, t.SourceID, t.Customer, t.Merchant} {
		if !idPattern.MatchString(id) {
			return errors.New("invalid identifier")
		}
	}
	if math.IsNaN(t.Amount) || math.IsInf(t.Amount, 0) || t.Amount <= 0 || t.Amount > 1e6 {
		return errors.New("amount must be finite and in (0,1000000]")
	}
	if math.Abs(t.Amount*100-math.Round(t.Amount*100)) > 1e-6 {
		return errors.New("amount must have at most two decimal places")
	}
	if t.Currency != "USD" || t.Type != "PAYMENT" {
		return errors.New("v1 supports USD PAYMENT events")
	}
	if t.Timestamp.IsZero() || t.Timestamp.Year() < 2000 || t.Timestamp.After(time.Now().Add(5*time.Minute)) {
		return errors.New("timestamp outside supported range")
	}
	if len(t.Device) > 96 {
		return errors.New("device_id too long")
	}
	return nil
}

type Envelope struct {
	Event       Transaction `json:"event"`
	AcceptedAt  time.Time   `json:"accepted_at"`
	Correlation string      `json:"correlation_id"`
}

func Decode(data []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return e, err
	}
	if err := e.Event.Validate(); err != nil {
		return e, err
	}
	e.Event.Timestamp = e.Event.Timestamp.UTC()
	if e.AcceptedAt.IsZero() {
		return e, errors.New("missing accepted_at")
	}
	return e, nil
}
