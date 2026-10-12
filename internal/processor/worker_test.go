package processor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryBounded(t *testing.T) {
	n := 0
	err := Retry(context.Background(), 3, time.Nanosecond, func() error { n++; return errors.New("offline") })
	if err == nil || n != 3 {
		t.Fatalf("err=%v attempts=%d", err, n)
	}
}
func TestRetryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n := 0
	err := Retry(ctx, 4, time.Hour, func() error { n++; return errors.New("offline") })
	if !errors.Is(err, context.Canceled) || n != 0 {
		t.Fatal("cancellation ignored")
	}
}
func TestRetryEventuallySucceeds(t *testing.T) {
	n := 0
	err := Retry(context.Background(), 4, time.Nanosecond, func() error {
		n++
		if n < 2 {
			return errors.New("timeout")
		}
		return nil
	})
	if err != nil || n != 2 {
		t.Fatal(err)
	}
}
