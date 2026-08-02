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
	EventType string          `json:"event_type,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	Attempts  int             `json:"attempts"`
	NextAt    time.Time       `json:"next_at"`
	CreatedAt time.Time       `json:"created_at"`
}

// ephemeralEventTypes — события, которые устаревают и которые можно отбрасывать,
// если они провисели в очереди дольше TTL (см. RunPublisher). Heartbeat и traffic
// отражают состояние на момент отправки; протухшими они бесполезны и лишь копятся.
// Результаты sync/revoke и ошибки НЕ эфемерны — их терять нельзя.
var ephemeralEventTypes = map[string]bool{
	"vpn.node_heartbeat": true,
	"vpn.node_traffic":   true,
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
	// Достаём type из уже сериализованного payload (все события несут поле "type").
	var probe struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(raw, &probe)

	q.mu.Lock()
	defer q.mu.Unlock()
	q.nextID++
	q.items = append(q.items, pendingEvent{
		ID:        q.nextID,
		Topic:     topic,
		Key:       key,
		EventType: probe.Type,
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

		// TTL для эфемерных событий: heartbeat/traffic старше этого срока
		// отбрасываем — они устарели и лишь копятся. Не эфемерные (sync/revoke/
		// ошибки) держим всегда.
		const ephemeralTTL = 5 * time.Minute

		q.mu.Lock()
		now := time.Now().UTC()
		var due []pendingEvent
		var keep []pendingEvent
		dropped := 0
		for _, it := range q.items {
			if ephemeralEventTypes[it.EventType] && now.Sub(it.CreatedAt) > ephemeralTTL {
				dropped++
				continue
			}
			if it.NextAt.After(now) {
				keep = append(keep, it)
				continue
			}
			due = append(due, it)
		}
		q.mu.Unlock()

		if dropped > 0 {
			slog.Info("node-agent dropped stale ephemeral pending events", "count", dropped)
			q.mu.Lock()
			_ = q.saveLocked()
			q.mu.Unlock()
		}

		if len(due) == 0 {
			if idle < 2*time.Second {
				idle *= 2
			}
			continue
		}
		idle = 200 * time.Millisecond

		var stillPending []pendingEvent
		consecutiveFail := 0
		resetDone := false
		for _, it := range due {
			publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := q.producer.PublishJSON(publishCtx, it.Topic, it.Key, jsonRaw(it.Payload))
			cancel()
			if err != nil {
				it.Attempts++
				it.NextAt = now.Add(time.Duration(minInt(60, 1<<minInt(it.Attempts, 6))) * time.Second)
				slog.Warn("node-agent pending publish failed", "topic", it.Topic, "attempts", it.Attempts, "err", err)
				stillPending = append(stillPending, it)

				// Серия подряд идущих неудач = writer, вероятно, залип на мёртвом
				// соединении. Один раз за проход пересоздаём его, чтобы сбросить
				// устаревший кэш брокеров. Следующий тик попробует уже свежий writer.
				consecutiveFail++
				if consecutiveFail >= 3 && !resetDone {
					slog.Warn("node-agent resetting kafka writer after consecutive publish failures")
					q.producer.Reset()
					resetDone = true
				}
				continue
			}
			consecutiveFail = 0
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
