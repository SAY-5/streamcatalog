// Package kafkax wraps the Kafka admin operations the catalog needs: creating a
// topic when a stream is registered and confirming a topic exists.
package kafkax

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Admin performs topic create/describe operations against a Kafka cluster.
type Admin struct {
	broker string
}

// New returns an Admin pointed at a single bootstrap broker.
func New(broker string) *Admin {
	return &Admin{broker: broker}
}

func (a *Admin) dial(ctx context.Context) (*kafka.Conn, error) {
	d := &kafka.Dialer{Timeout: 10 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", a.broker)
	if err != nil {
		return nil, fmt.Errorf("dial kafka: %w", err)
	}
	controller, err := conn.Controller()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("controller: %w", err)
	}
	_ = conn.Close()
	cc, err := d.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return nil, fmt.Errorf("dial controller: %w", err)
	}
	return cc, nil
}

// CreateTopic creates a topic. It is idempotent: an existing topic is not an
// error, which avoids a race when registration retries.
func (a *Admin) CreateTopic(ctx context.Context, topic string, partitions int) error {
	conn, err := a.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	cfg := kafka.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}
	if createErr := conn.CreateTopics(cfg); createErr != nil {
		if errors.Is(createErr, kafka.TopicAlreadyExists) {
			return nil
		}
		return fmt.Errorf("create topic: %w", createErr)
	}
	return nil
}

// TopicExists reports whether a topic is present in cluster metadata.
func (a *Admin) TopicExists(ctx context.Context, topic string) (bool, error) {
	conn, err := a.dial(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = conn.Close() }()
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return false, nil
	}
	return len(partitions) > 0, nil
}
