package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

var ErrDisabled = errors.New("kafka producer disabled")

type Producer struct {
	writer  *kafkago.Writer
	enabled bool
}

func NewProducer(brokers []string) *Producer {
	if len(brokers) == 0 {
		slog.Warn("kafka producer disabled: no brokers provided")
		return &Producer{enabled: false}
	}

	return &Producer{
		enabled: true,
		writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Balancer:               &kafkago.Hash{},
			RequiredAcks:           kafkago.RequireAll,
			BatchTimeout:           50 * time.Millisecond,
			AllowAutoTopicCreation: false,
		},
	}
}

func (p *Producer) Close() error {
	if !p.enabled || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

func (p *Producer) PublishJSON(ctx context.Context, topic, key string, v any) error {
	if !p.enabled || p.writer == nil {
		return ErrDisabled
	}
	if topic == "" {
		return fmt.Errorf("empty kafka topic")
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}

	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal kafka json: %w", err)
	}

	start := time.Now()
	err = p.writer.WriteMessages(ctx, kafkago.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
		Time:  time.Now().UTC(),
	})
	if err != nil {
		slog.Error("kafka publish failed", "topic", topic, "key", key, "duration", time.Since(start), "err", err)
		return err
	}
	slog.Debug("kafka publish ok", "topic", topic, "key", key, "duration", time.Since(start))
	return nil
}
