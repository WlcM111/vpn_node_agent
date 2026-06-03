package vpn_node_agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	commonkafka "vpn_node_agent/internal/common/kafka"
)

type pendingEvent struct {
	ID        int64           `json:"id"`
	Topic     string          `json:"topic"`
	Key       string          `json:"key"`
	Payload   json.RawMessage `json:"payload"`
	Attempts  int             `json:"attempts"`
	NextAt    time.Time       `json:"next_at"`
	CreatedAt time.Time       `json:"created_at"`
}

type PendingQueue struct {
	mu       sync.Mutex
	path     string
	items    []pendingEvent
	nextID   int64
	producer *commonkafka.Producer
}

func NewPendingQueue(path string, producer *commonkafka.Producer) (*PendingQueue, error) {
	q := &PendingQueue{path: path, producer: producer}
	if err := q.load(); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *PendingQueue) load() error {
	data, err := os.ReadFile(q.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read pending events: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &q.items); err != nil {
		return fmt.Errorf("decode pending events: %w", err)
	}
	for _, it := range q.items {
		if it.ID > q.nextID {
			q.nextID = it.ID
		}
	}
	return nil
}

func (q *PendingQueue) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(q.path), 0o750); err != nil {
		return fmt.Errorf("mkdir pending dir: %w", err)
	}
	data, err := json.MarshalIndent(q.items, "", "  ")
	if err != nil {
		return fmt.Errorf("encode pending: %w", err)
	}
	tmp := q.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write tmp pending: %w", err)
	}
	if err := os.Rename(tmp, q.path); err != nil {
		return fmt.Errorf("rename pending: %w", err)
	}
	return nil
}

func (q *PendingQueue) Enqueue(topic, key string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal pending payload: %w", err)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.nextID++
	q.items = append(q.items, pendingEvent{
		ID:        q.nextID,
		Topic:     topic,
		Key:       key,
		Payload:   raw,
		NextAt:    time.Now().UTC(),
		CreatedAt: time.Now().UTC(),
	})
	return q.saveLocked()
}

func (q *PendingQueue) RunPublisher(ctx context.Context) {
	if q.producer == nil {
		return
	}
	idle := 200 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(idle):
		}

		q.mu.Lock()
		now := time.Now().UTC()
		var due []pendingEvent
		var keep []pendingEvent
		for _, it := range q.items {
			if it.NextAt.After(now) {
				keep = append(keep, it)
				continue
			}
			due = append(due, it)
		}
		q.mu.Unlock()

		if len(due) == 0 {
			if idle < 2*time.Second {
				idle *= 2
			}
			continue
		}
		idle = 200 * time.Millisecond

		var stillPending []pendingEvent
		for _, it := range due {
			publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := q.producer.PublishJSON(publishCtx, it.Topic, it.Key, jsonRaw(it.Payload))
			cancel()
			if err != nil {
				it.Attempts++
				it.NextAt = now.Add(time.Duration(minInt(60, 1<<minInt(it.Attempts, 6))) * time.Second)
				slog.Warn("node-agent pending publish failed", "topic", it.Topic, "attempts", it.Attempts, "err", err)
				stillPending = append(stillPending, it)
			}
		}

		q.mu.Lock()
		q.items = append(keep, stillPending...)
		_ = q.saveLocked()
		q.mu.Unlock()
	}
}

type jsonRaw []byte

func (j jsonRaw) MarshalJSON() ([]byte, error) { return j, nil }

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
