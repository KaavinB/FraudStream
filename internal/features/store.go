package features

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/KaavinB/FraudStream/internal/event"
	"github.com/redis/go-redis/v9"
)

//go:embed rolling.lua
var Lua string

const Version = "prior-v1"

type Store struct {
	Client    *redis.Client
	Namespace string
}

func (s Store) Keys(t event.Transaction) []string {
	return []string{s.Namespace + ":c:" + t.Customer + ":history", s.Namespace + ":c:" + t.Customer + ":meta", s.Namespace + ":m:" + t.Merchant + ":history", s.Namespace + ":m:" + t.Merchant + ":meta"}
}
func (s Store) Call(ctx context.Context, mode string, t event.Transaction) (string, error) {
	stamp := float64(t.Timestamp.UnixNano()) / 1e9
	weekday := (int(t.Timestamp.UTC().Weekday()) + 6) % 7
	return redis.NewScript(Lua).Run(ctx, s.Client, s.Keys(t), mode, fmt.Sprintf("%.9f", stamp), t.Amount, t.Customer, t.Merchant, t.ID, t.Timestamp.UTC().Hour(), weekday).Text()
}
func (s Store) Snapshot(ctx context.Context, t event.Transaction) ([]float64, error) {
	raw, err := s.Call(ctx, "SNAPSHOT", t)
	if err != nil {
		return nil, err
	}
	var values []float64
	err = json.Unmarshal([]byte(raw), &values)
	return values, err
}
func (s Store) Apply(ctx context.Context, t event.Transaction) error {
	_, err := s.Call(ctx, "APPLY", t)
	return err
}
