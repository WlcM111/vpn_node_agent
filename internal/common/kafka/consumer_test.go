package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// fakeReader отдаёт сообщения по порядку и запоминает коммиты. Когда
// сообщения кончились — отменяет контекст, как остановка сервиса.
type fakeReader struct {
	mu        sync.Mutex
	msgs      []kafkago.Message
	pos       int
	committed []int64
	cancel    context.CancelFunc
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ctx.Err() != nil {
		return kafkago.Message{}, ctx.Err()
	}
	if f.pos >= len(f.msgs) {
		f.cancel()
		return kafkago.Message{}, context.Canceled
	}
	m := f.msgs[f.pos]
	f.pos++
	return m, nil
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range msgs {
		f.committed = append(f.committed, m.Offset)
	}
	return nil
}

func fastRetries(t *testing.T) {
	t.Helper()
	oldBackoff, oldWait := retryBackoff, dltRetryWait
	retryBackoff = func(int) time.Duration { return time.Millisecond }
	dltRetryWait = time.Millisecond
	t.Cleanup(func() { retryBackoff, dltRetryWait = oldBackoff, oldWait })
}

func twoMessages() []kafkago.Message {
	now := time.Now()
	return []kafkago.Message{
		{Topic: "vpn.commands", Key: []byte("1"), Offset: 10, Time: now},
		{Topic: "vpn.commands", Key: []byte("1"), Offset: 11, Time: now.Add(time.Millisecond)},
	}
}

// Упавшее сообщение повторяется на месте и коммитится раньше следующего.
func TestConsumerRetriesSameMessageInPlace(t *testing.T) {
	fastRetries(t)
	ctx, cancel := context.WithCancel(context.Background())
	r := &fakeReader{msgs: twoMessages(), cancel: cancel}
	var calls []int64
	fails := 3
	err := runConsumer(ctx, r, "test", func(_ context.Context, m Message) error {
		calls = append(calls, m.Offset)
		if m.Offset == 10 && fails > 0 {
			fails--
			return errors.New("transient")
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int64{10, 10, 10, 10, 11}; !equal(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	if !equal(r.committed, []int64{10, 11}) {
		t.Fatalf("committed = %v", r.committed)
	}
}

// D9: пока DLT недоступен, следующее сообщение не читается и не коммитится —
// иначе упавшее сообщение терялось бы.
func TestConsumerDoesNotSkipWhenDLTUnavailable(t *testing.T) {
	fastRetries(t)
	ctx, cancel := context.WithCancel(context.Background())
	r := &fakeReader{msgs: twoMessages(), cancel: cancel}
	dltCalls := 0
	var handled []int64
	done := make(chan error, 1)
	go func() {
		done <- runConsumer(ctx, r, "test", func(_ context.Context, m Message) error {
			handled = append(handled, m.Offset)
			if m.Offset == 10 {
				return errors.New("poison")
			}
			return nil
		}, func(context.Context, Message, error) error {
			dltCalls++
			if dltCalls == 50 {
				cancel() // остановка сервиса, DLT так и не стал доступен
			}
			return errors.New("kafka down")
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop")
	}
	for _, off := range handled {
		if off == 11 {
			t.Fatal("next message processed while the failed one was not in DLT")
		}
	}
	if len(r.committed) != 0 {
		t.Fatalf("committed %v while DLT was unavailable", r.committed)
	}
}

// Сообщение, исчерпавшее попытки, коммитится только после успешной
// публикации в DLT; следующее обрабатывается после этого.
func TestConsumerCommitsAfterDLTSucceeds(t *testing.T) {
	fastRetries(t)
	ctx, cancel := context.WithCancel(context.Background())
	r := &fakeReader{msgs: twoMessages(), cancel: cancel}
	dltCalls := 0
	var handled []int64
	err := runConsumer(ctx, r, "test", func(_ context.Context, m Message) error {
		handled = append(handled, m.Offset)
		if m.Offset == 10 {
			return errors.New("poison")
		}
		return nil
	}, func(context.Context, Message, error) error {
		dltCalls++
		if dltCalls < 3 {
			return errors.New("kafka down")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if dltCalls != 3 || !equal(r.committed, []int64{10, 11}) || handled[len(handled)-1] != 11 {
		t.Fatalf("dlt=%d committed=%v handled=%v", dltCalls, r.committed, handled)
	}
	var poison int
	for _, h := range handled {
		if h == 10 {
			poison++
		}
	}
	if poison != 20 {
		t.Fatalf("poison message attempts = %d, want 20", poison)
	}
}

func equal(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
