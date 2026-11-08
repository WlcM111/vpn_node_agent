package vpn_node_agent

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	commonkafka "vpn_node_agent/internal/common/kafka"
)

// stallingBroker принимает TCP-соединения и ничего не отвечает: запись в
// Kafka висит до таймаута публикации (5 с). Так проход публикации гарантированно
// длится, пока тест ставит в очередь новое событие.
func stallingBroker(t *testing.T) (addr string, accepted <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan struct{}, 16)
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns = append(conns, c)
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return ln.Addr().String(), ch
}

func readPendingFile(t *testing.T, path string) []pendingEvent {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var items []pendingEvent
	if err := json.Unmarshal(data, &items); err != nil {
		t.Fatal(err)
	}
	return items
}

// Дефект D-node-3: событие, поставленное в очередь, пока RunPublisher
// публиковал пачку, терялось — в конце прохода очередь перезаписывалась
// снимком начала прохода. Терялись в том числе результаты sync/revoke.
func TestPendingQueueKeepsEventsEnqueuedDuringPublish(t *testing.T) {
	addr, accepted := stallingBroker(t)
	producer := commonkafka.NewProducer([]string{addr})
	t.Cleanup(func() { _ = producer.Close() })

	path := filepath.Join(t.TempDir(), "pending.json")
	q, err := NewPendingQueue(path, producer)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(commonkafka.TopicVPNEvents, "node-1", map[string]any{"type": "vpn.node_user_synced", "n": 1}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.RunPublisher(ctx)

	select {
	case <-accepted: // публикация первого события идёт
	case <-time.After(10 * time.Second):
		t.Fatal("publisher did not try to publish")
	}
	if err := q.Enqueue(commonkafka.TopicVPNEvents, "node-1", map[string]any{"type": "vpn.node_user_synced", "n": 2}); err != nil {
		t.Fatal(err)
	}

	// Ждём конца прохода: первое событие получит attempts=1.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		items := readPendingFile(t, path)
		var first, second *pendingEvent
		for i := range items {
			var p struct {
				N int `json:"n"`
			}
			_ = json.Unmarshal(items[i].Payload, &p)
			switch p.N {
			case 1:
				first = &items[i]
			case 2:
				second = &items[i]
			}
		}
		if first != nil && first.Attempts >= 1 {
			if second == nil {
				t.Fatalf("event enqueued during publish was lost: queue = %+v", items)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("publish pass did not finish in time")
}

// Слияние после прохода: взятые в проход убираются, неудачные возвращаются,
// добавленные во время прохода остаются, порядок — по времени постановки.
func TestMergePending(t *testing.T) {
	ev := func(id int64, attempts int) pendingEvent { return pendingEvent{ID: id, Attempts: attempts} }
	current := []pendingEvent{ev(1, 0), ev(2, 0), ev(3, 0), ev(4, 0)} // 4 — добавлено во время прохода
	taken := []pendingEvent{ev(1, 0), ev(3, 0)}
	failed := []pendingEvent{ev(3, 1)}
	got := mergePending(current, taken, failed)
	want := []pendingEvent{ev(2, 0), ev(3, 1), ev(4, 0)}
	if len(got) != len(want) {
		t.Fatalf("merge = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].Attempts != want[i].Attempts {
			t.Fatalf("merge = %+v, want %+v", got, want)
		}
	}
}
