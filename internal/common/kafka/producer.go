package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

var ErrDisabled = errors.New("kafka producer disabled")

type Producer struct {
	mu      sync.Mutex
	brokers []string
	writer  *kafkago.Writer
	enabled bool
	// closed — Close уже вызван: Reset не должен воскрешать writer, а
	// публикация после остановки возвращает ErrDisabled.
	closed bool
}

func newWriter(addr net.Addr) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   addr,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireAll,
		BatchTimeout:           50 * time.Millisecond,
		AllowAutoTopicCreation: false,
		// Общий транспорт процесса (TLS, если настроен): свой транспорт на
		// каждый writer оставлял бы пул соединений после пересоздания.
		Transport: TransportFromEnv(),
	}
}

func NewProducer(brokers []string) *Producer {
	if len(brokers) == 0 {
		slog.Warn("kafka producer disabled: no brokers provided")
		return &Producer{enabled: false}
	}

	cp := append([]string(nil), brokers...)
	return &Producer{
		enabled: true,
		brokers: cp,
		writer:  newWriter(kafkago.TCP(cp...)),
	}
}

func (p *Producer) Close() error {
	p.mu.Lock()
	w := p.writer
	p.writer = nil
	p.closed = true
	p.mu.Unlock()
	if !p.enabled || w == nil {
		return nil
	}
	return w.Close()
}

// Reset пересоздаёт внутренний writer с теми же брокерами. Нужен, когда writer
// «залип» на устаревшем соединении (после рестарта Kafka/центра) и каждая запись
// упирается в таймаут: новый writer начинает с чистого состояния (очереди
// партиций, незавершённые батчи), а соединения и метаданные брокеров kafka-go
// переустанавливает сам. Безопасен при конкурентной публикации; после Close
// ничего не делает.
func (p *Producer) Reset() {
	p.mu.Lock()
	if !p.enabled || p.closed {
		p.mu.Unlock()
		return
	}
	old := p.writer
	var addr net.Addr
	switch {
	case len(p.brokers) > 0:
		addr = kafkago.TCP(p.brokers...)
	case old != nil:
		addr = old.Addr
	default:
		// Писателя нет и брокеры неизвестны — пересоздавать нечего.
		p.mu.Unlock()
		return
	}
	p.writer = newWriter(addr)
	p.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

func (p *Producer) currentWriter() *kafkago.Writer {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.enabled || p.closed {
		return nil
	}
	return p.writer
}

func (p *Producer) PublishJSON(ctx context.Context, topic, key string, v any) error {
	w := p.currentWriter()
	if w == nil {
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
	err = w.WriteMessages(ctx, kafkago.Message{
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
