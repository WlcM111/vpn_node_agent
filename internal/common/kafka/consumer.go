package kafka

import (
	"context"
	"errors"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type Message = kafkago.Message

type Handler func(ctx context.Context, msg Message) error

var ErrSkip = errors.New("kafka: skip message")

func NewReader(brokers []string, topic, groupID string) *kafkago.Reader {
	if len(brokers) == 0 {
		slog.Warn("kafka consumer disabled: no brokers provided", "topic", topic, "group_id", groupID)
		return nil
	}

	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0,
		StartOffset:    kafkago.FirstOffset,
	})
}

func RunConsumerWithDLT(ctx context.Context, reader *kafkago.Reader, serviceName string, handle Handler, dltProducer *Producer, dltTopic string) error {
	if reader == nil {
		return nil
	}
	if handle == nil {
		return errors.New("nil kafka handler")
	}

	attempts := map[string]int{}

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return err
		}

		attemptKey := msg.Topic + ":" + string(msg.Key) + ":" + time.Unix(0, msg.Time.UnixNano()).UTC().Format(time.RFC3339Nano)

		err = handle(ctx, msg)
		if err != nil {
			if errors.Is(err, ErrSkip) {
				_ = publishDLT(ctx, dltProducer, dltTopic, msg, serviceName, err)
				delete(attempts, attemptKey)
				if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
					return commitErr
				}
				continue
			}

			attempts[attemptKey]++
			slog.Error("kafka message handling failed",
				"service", serviceName,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"attempt", attempts[attemptKey],
				"err", err,
			)

			if attempts[attemptKey] >= 20 && dltProducer != nil && dltTopic != "" {
				_ = publishDLT(ctx, dltProducer, dltTopic, msg, serviceName, err)
				delete(attempts, attemptKey)
				if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
					return commitErr
				}
				continue
			}

			time.Sleep(time.Duration(minInt(attempts[attemptKey], 10)) * 500 * time.Millisecond)
			continue
		}

		delete(attempts, attemptKey)
		if err := reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func publishDLT(ctx context.Context, producer *Producer, topic string, msg Message, serviceName string, cause error) error {
	if producer == nil || topic == "" {
		return nil
	}
	payload := map[string]any{
		"service":    serviceName,
		"topic":      msg.Topic,
		"partition":  msg.Partition,
		"offset":     msg.Offset,
		"key":        string(msg.Key),
		"value":      string(msg.Value),
		"error":      cause.Error(),
		"created_at": time.Now().UTC(),
	}
	return producer.PublishJSON(ctx, topic, string(msg.Key), payload)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
