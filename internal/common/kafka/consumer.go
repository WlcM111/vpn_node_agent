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
		Dialer:         DialerFromEnv(),
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0,
		StartOffset:    kafkago.FirstOffset,
	})
}

// messageReader — то, что консьюмеру нужно от kafka-go Reader. Выделено,
// чтобы цикл обработки можно было проверить тестом без брокера.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// maxAttempts — после стольких неудачных попыток сообщение уходит в DLT.
const maxAttempts = 20

// retryBackoff — пауза перед повтором того же сообщения; dltRetryWait —
// пауза перед повтором публикации в DLT. Переменные — для тестов.
var (
	retryBackoff = func(attempt int) time.Duration {
		return time.Duration(minInt(attempt, 10)) * 500 * time.Millisecond
	}
	dltRetryWait = 5 * time.Second
)

func RunConsumerWithDLT(ctx context.Context, reader *kafkago.Reader, serviceName string, handle Handler, dltProducer *Producer, dltTopic string) error {
	if reader == nil {
		return nil
	}
	var dlt func(context.Context, Message, error) error
	if dltProducer != nil && dltTopic != "" {
		dlt = func(ctx context.Context, msg Message, cause error) error {
			return publishDLT(ctx, dltProducer, dltTopic, msg, serviceName, cause)
		}
	}
	return runConsumer(ctx, reader, serviceName, handle, dlt)
}

// runConsumer обрабатывает сообщения строго по порядку.
//
// Упавшее сообщение повторяется НА МЕСТЕ — до успеха или до maxAttempts, после
// чего уходит в DLT. Раньше после ошибки цикл делал continue: FetchMessage
// отдавал СЛЕДУЮЩЕЕ сообщение, а его коммит сдвигал offset группы дальше
// упавшего — команда (например, отзыв доступа пользователя) терялась до
// рестарта агента и навсегда после первого же коммита следующей команды.
// Offset коммитится только после успеха или успешной публикации в DLT.
func runConsumer(ctx context.Context, reader messageReader, serviceName string, handle Handler, dlt func(context.Context, Message, error) error) error {
	if handle == nil {
		return errors.New("nil kafka handler")
	}

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			// Транзиентная ошибка fetch (брокер недоступен, потеря координатора
			// группы, rebalance после рестарта центра). НЕ убиваем цикл — kafka-go
			// сам переустановит соединение при следующем FetchMessage. Небольшая
			// пауза, чтобы не крутить busy-loop, и продолжаем.
			slog.Warn("kafka fetch failed, will retry",
				"service", serviceName, "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
			continue
		}

		err = handle(ctx, msg)
		for attempt := 1; err != nil && !errors.Is(err, ErrSkip) && attempt < maxAttempts; attempt++ {
			slog.Error("kafka message handling failed",
				"service", serviceName,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"attempt", attempt,
				"err", err,
			)
			select {
			case <-ctx.Done():
				// Остановка: offset не закоммичен, сообщение придёт снова.
				return nil
			case <-time.After(retryBackoff(attempt)):
			}
			err = handle(ctx, msg)
		}

		if err != nil {
			if dlt != nil && !publishDLTUntilDone(ctx, dlt, msg, serviceName, err) {
				return nil
			}
		}

		if !commitUntilDone(ctx, reader, msg, serviceName) {
			return nil
		}
	}
}

// publishDLTUntilDone публикует сообщение в DLT, повторяя до успеха: пока
// DLT недоступен, следующее сообщение не читается (иначе упавшее потеряется).
// false — контекст отменён.
func publishDLTUntilDone(ctx context.Context, dlt func(context.Context, Message, error) error, msg Message, serviceName string, cause error) bool {
	for {
		dltErr := dlt(ctx, msg, cause)
		if dltErr == nil {
			return true
		}
		slog.Error("failed to publish message to DLT, keeping offset uncommitted",
			"service", serviceName, "topic", msg.Topic, "partition", msg.Partition,
			"offset", msg.Offset, "key", string(msg.Key), "err", dltErr)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(dltRetryWait):
		}
	}
}

// commitUntilDone коммитит offset, повторяя при транзиентной ошибке.
// Сообщение уже обработано (идемпотентно), повторная доставка отсеется
// дедупом по command_id. false — контекст отменён.
func commitUntilDone(ctx context.Context, reader messageReader, msg Message, serviceName string) bool {
	for {
		err := reader.CommitMessages(ctx, msg)
		if err == nil {
			return true
		}
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return false
		}
		slog.Warn("kafka commit failed, will retry", "service", serviceName, "err", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
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
