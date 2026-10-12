package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KaavinB/FraudStream/internal/event"
	"github.com/KaavinB/FraudStream/internal/features"
	"github.com/KaavinB/FraudStream/internal/storage"
	"github.com/KaavinB/FraudStream/internal/telemetry"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Worker struct {
	Client       *kgo.Client
	DB           *storage.DB
	Features     features.Store
	HTTP         *http.Client
	InferenceURL string
	Concurrency  int
	CrashAt      string
	CrashID      string
}

func (w *Worker) crash(stage string, id string) {
	if w.CrashAt == stage && (w.CrashID == "" || w.CrashID == id) {
		os.Exit(86)
	}
}
func (w *Worker) score(ctx context.Context, id string, values []float64) (storage.Score, error) {
	var score storage.Score
	raw, _ := json.Marshal(map[string]any{"transaction_id": id, "feature_version": features.Version, "features": values})
	req, err := http.NewRequestWithContext(ctx, "POST", w.InferenceURL+"/predict", bytes.NewReader(raw))
	if err != nil {
		return score, err
	}
	req.Header.Set("Content-Type", "application/json")
	began := time.Now()
	response, err := w.HTTP.Do(req)
	if err != nil {
		return score, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return score, fmt.Errorf("inference status %d", response.StatusCode)
	}
	if err = json.NewDecoder(io.LimitReader(response.Body, 16384)).Decode(&score); err != nil {
		return score, err
	}
	if score.ID != id || math.IsNaN(score.Risk) || math.IsInf(score.Risk, 0) || score.Risk < 0 || score.Risk > 1 || score.Version == "" || score.Timestamp.IsZero() || (score.Band != "ALLOW" && score.Band != "REVIEW" && score.Band != "BLOCK") {
		return score, errors.New("invalid inference response")
	}
	telemetry.Duration.WithLabelValues("inference_http").Observe(time.Since(began).Seconds())
	return score, nil
}
func (w *Worker) Process(ctx context.Context, r *kgo.Record) error {
	envelope, err := event.Decode(r.Value)
	if err != nil {
		return w.DB.Quarantine(ctx, r.Topic, r.Partition, r.Offset, "INVALID_SCHEMA", r.Value)
	}
	complete, err := w.DB.Complete(ctx, envelope.Event.ID)
	if err != nil {
		return err
	}
	if complete {
		previous, err := w.DB.Recent(ctx, envelope.Event.ID, 1)
		if err != nil {
			return err
		}
		if len(previous) != 1 {
			return errors.New("completed transaction missing")
		}
		a, _ := json.Marshal(previous[0].Transaction)
		b, _ := json.Marshal(envelope.Event)
		if string(a) != string(b) {
			return w.DB.Quarantine(ctx, r.Topic, r.Partition, r.Offset, "CONFLICTING_ID", r.Value)
		}
		telemetry.Events.WithLabelValues("processor", "duplicate").Inc()
		return nil
	}
	began := time.Now()
	values, featureSeconds, err := w.DB.Prepare(ctx, w.Features, envelope, func(s string) { w.crash(s, envelope.Event.ID) })
	if err != nil {
		if strings.Contains(err.Error(), "LATE_EVENT") || strings.Contains(err.Error(), "CONFLICTING_ID") || strings.Contains(err.Error(), "CAPACITY_EXCEEDED") || strings.Contains(err.Error(), "transactions_source_event_id_key") {
			telemetry.Events.WithLabelValues("processor", "quarantine").Inc()
			return w.DB.Quarantine(ctx, r.Topic, r.Partition, r.Offset, "EVENT_POLICY: "+err.Error(), r.Value)
		}
		return err
	}
	telemetry.Duration.WithLabelValues("features").Observe(featureSeconds)
	telemetry.Duration.WithLabelValues("accepted_to_materialization").Observe(time.Since(envelope.AcceptedAt).Seconds())
	if !r.Timestamp.IsZero() {
		telemetry.Duration.WithLabelValues("kafka_to_materialization").Observe(time.Since(r.Timestamp).Seconds())
	}
	w.crash("before_inference", envelope.Event.ID)
	score, err := w.score(ctx, envelope.Event.ID, values)
	if err != nil {
		return err
	}
	w.crash("after_inference", envelope.Event.ID)
	persistence := time.Now()
	if err = w.DB.Persist(ctx, envelope, score, featureSeconds); err != nil {
		return err
	}
	telemetry.Duration.WithLabelValues("database").Observe(time.Since(persistence).Seconds())
	w.crash("after_persistence", envelope.Event.ID)
	telemetry.Events.WithLabelValues("processor", "completed").Inc()
	telemetry.Duration.WithLabelValues("processing").Observe(time.Since(began).Seconds())
	telemetry.Duration.WithLabelValues("end_to_end").Observe(time.Since(envelope.AcceptedAt).Seconds())
	return nil
}

// Retry bounds each attempt series. Exhausted infrastructure failures stop the worker,
// preserving the failed record and later offsets for recovery instead of dropping work.
func Retry(ctx context.Context, attempts int, base time.Duration, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err = fn(); err == nil {
			return nil
		}
		telemetry.Events.WithLabelValues("processor", "retry").Inc()
		if i+1 == attempts {
			break
		}
		timer := time.NewTimer(base * time.Duration(1<<i))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func (w *Worker) Run(ctx context.Context) error {
	slots := make(chan struct{}, w.Concurrency)
	for ctx.Err() == nil {
		fetched := w.Client.PollRecords(ctx, 256)
		if err := fetched.Err(); err != nil {
			w.Client.AllowRebalance()
			return err
		}
		var wg sync.WaitGroup
		failures := make(chan error, 256)
		fetched.EachPartition(func(p kgo.FetchTopicPartition) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case slots <- struct{}{}:
					defer func() { <-slots }()
				case <-ctx.Done():
					failures <- ctx.Err()
					return
				}
				// Each partition is processed serially. One commit per fetched batch covers the
				// last durable record; a failure commits only the prefix before it, so no commit
				// can overtake a failure. A per-record synchronous commit capped throughput.
				var done *kgo.Record
				commit := func() error {
					if done == nil {
						return nil
					}
					began := time.Now()
					if err := w.Client.CommitRecords(ctx, done); err != nil {
						return err
					}
					telemetry.Duration.WithLabelValues("offset_commit").Observe(time.Since(began).Seconds())
					telemetry.Lag.WithLabelValues(done.Topic, strconv.Itoa(int(done.Partition))).Set(float64(p.HighWatermark - done.Offset - 1))
					return nil
				}
				for _, record := range p.Records {
					err := Retry(ctx, 4, 100*time.Millisecond, func() error {
						attempt, cancel := context.WithTimeout(ctx, 15*time.Second)
						defer cancel()
						return w.Process(attempt, record)
					})
					if err != nil {
						if commitErr := commit(); commitErr != nil {
							err = errors.Join(err, commitErr)
						}
						failures <- err
						return
					}
					w.crash("before_offset_commit", func() string { e, _ := event.Decode(record.Value); return e.Event.ID }())
					done = record
				}
				if err := commit(); err != nil {
					failures <- err
				}
			}()
		})
		wg.Wait()
		close(failures)
		w.Client.AllowRebalance()
		for err := range failures {
			if err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}
