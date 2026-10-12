package kafka

import (
	"context"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func Producer(brokers string) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerLinger(2*time.Millisecond), kgo.RecordDeliveryTimeout(5*time.Second), kgo.MaxBufferedRecords(10000))
}

// Consumer uses a short group session so a crashed member (which never sends LeaveGroup)
// releases its partitions quickly. The franz-go default of 45s dominated measured crash recovery.
func Consumer(brokers, group, topic string, session time.Duration) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.FetchMaxBytes(4<<20), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.SessionTimeout(session), kgo.HeartbeatInterval(session/6))
}
func Publish(ctx context.Context, client *kgo.Client, topic, key string, data []byte) error {
	return client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: data}).FirstErr()
}
