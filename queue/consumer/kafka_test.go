package consumer

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/chihqiang/cdc-go/types"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

// fakeKafkaReader serves the messages it was given, records the offsets that
// were committed, and then blocks the way a real reader waits for the next
// message.
type fakeKafkaReader struct {
	mu        sync.Mutex
	messages  []kafka.Message
	next      int
	committed []int64
	readErr   error
	closeErr  error
	closed    bool

	// done is closed by Close, which is what ends a read that is waiting.
	done chan struct{}
}

func newFakeKafkaReader(messages ...kafka.Message) *fakeKafkaReader {
	return &fakeKafkaReader{messages: messages, done: make(chan struct{})}
}

func (f *fakeKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	f.mu.Lock()
	if f.readErr != nil {
		err := f.readErr
		f.mu.Unlock()
		return kafka.Message{}, err
	}
	if f.next < len(f.messages) {
		msg := f.messages[f.next]
		f.next++
		f.mu.Unlock()
		return msg, nil
	}
	f.mu.Unlock()

	// Nothing left to serve: wait for the next message, like the reader does.
	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	case <-f.done:
		return kafka.Message{}, io.EOF
	}
}

func (f *fakeKafkaReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, msg := range msgs {
		f.committed = append(f.committed, msg.Offset)
	}
	return nil
}

func (f *fakeKafkaReader) Close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	close(f.done)
	return f.closeErr
}

func (f *fakeKafkaReader) committedOffsets() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.committed...)
}

// kafkaMessage is one message as the producer would have written it.
func kafkaMessage(t *testing.T, offset int64, payload []byte) kafka.Message {
	t.Helper()
	return kafka.Message{
		Topic:     "events",
		Partition: 0,
		Offset:    offset,
		Value:     payload,
	}
}

func newFakeConsumer(t *testing.T, cfg KafkaConfig, reader *fakeKafkaReader) *KafkaConsumer {
	t.Helper()
	c := &KafkaConsumer{cfg: cfg, reader: reader}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// ---- constructor tests ----

func TestNewKafkaConsumer_NeedsBrokers(t *testing.T) {
	_, err := NewKafkaConsumer(KafkaConfig{Topic: "events"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "brokers")
}

func TestNewKafkaConsumer_UnknownStartOffset(t *testing.T) {
	_, err := NewKafkaConsumer(KafkaConfig{
		Brokers:     []string{"127.0.0.1:9092"},
		Topic:       "events",
		StartOffset: "middle",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "start_offset")
}

func TestNewKafkaConsumer_BuildsAReader(t *testing.T) {
	c, err := NewKafkaConsumer(KafkaConfig{
		Brokers:     []string{"127.0.0.1:9092"},
		Topic:       "events",
		Group:       "cdc-go",
		StartOffset: "last",
	})
	assert.NoError(t, err)
	assert.NotNil(t, c.reader)
	assert.NoError(t, c.Close())
}

func TestKafkaStartOffset(t *testing.T) {
	first := []string{"", "first", "earliest", "oldest", " First "}
	for _, name := range first {
		offset, err := kafkaStartOffset(name)
		assert.NoError(t, err, "name %q", name)
		assert.Equal(t, kafka.FirstOffset, offset, "name %q", name)
	}

	last := []string{"last", "latest", "newest", "LAST"}
	for _, name := range last {
		offset, err := kafkaStartOffset(name)
		assert.NoError(t, err, "name %q", name)
		assert.Equal(t, kafka.LastOffset, offset, "name %q", name)
	}

	_, err := kafkaStartOffset("middle")
	assert.Error(t, err)
}

// ---- Consume tests ----

func TestKafkaConsumer_Consume_NilHandler(t *testing.T) {
	c := newFakeConsumer(t, KafkaConfig{Group: "cdc-go"}, newFakeKafkaReader())

	assert.ErrorIs(t, c.Consume(context.Background(), nil), errNilHandler)
}

func TestKafkaConsumer_Consume_CommitsWhatTheHandlerAccepted(t *testing.T) {
	reader := newFakeKafkaReader(
		kafkaMessage(t, 1, pushedPayload(t, "first")),
		kafkaMessage(t, 2, pushedPayload(t, "second")),
	)
	c := newFakeConsumer(t, KafkaConfig{Group: "cdc-go"}, reader)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var tables []string
	consumed := make(chan error, 1)
	go func() {
		consumed <- c.Consume(ctx, func(_ context.Context, event types.EventData) error {
			mu.Lock()
			tables = append(tables, event.Row.Table)
			finished := len(tables) == 2
			mu.Unlock()
			if finished {
				cancel()
			}
			return nil
		})
	}()

	select {
	case err := <-consumed:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not return after the context was canceled")
	}

	mu.Lock()
	assert.Equal(t, []string{"first", "second"}, tables)
	mu.Unlock()
	// The offset is committed once the handler is done with the message, so a
	// crash cannot lose an event that was handled.
	assert.Equal(t, []int64{1, 2}, reader.committedOffsets())
}

func TestKafkaConsumer_Consume_LeavesARejectedOffsetUncommitted(t *testing.T) {
	reader := newFakeKafkaReader(
		kafkaMessage(t, 1, pushedPayload(t, "failing")),
		kafkaMessage(t, 2, pushedPayload(t, "working")),
	)
	c := newFakeConsumer(t, KafkaConfig{Group: "cdc-go"}, reader)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var seen []string
	consumed := make(chan error, 1)
	go func() {
		consumed <- c.Consume(ctx, func(_ context.Context, event types.EventData) error {
			mu.Lock()
			seen = append(seen, event.Row.Table)
			finished := len(seen) == 2
			mu.Unlock()
			if finished {
				cancel()
			}
			if event.Row.Table == "failing" {
				return errors.New("the downstream is down")
			}
			return nil
		})
	}()

	select {
	case err := <-consumed:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("a rejected message stopped the consumption")
	}

	mu.Lock()
	assert.Equal(t, []string{"failing", "working"}, seen)
	mu.Unlock()
	// Only the message that was handled is committed: the rejected one has to be
	// read again after a rebalance or a restart.
	assert.Equal(t, []int64{2}, reader.committedOffsets())
}

func TestKafkaConsumer_Consume_DropsAndCommitsUndecodablePayloads(t *testing.T) {
	reader := newFakeKafkaReader(
		kafkaMessage(t, 1, []byte("{not an event")),
		kafkaMessage(t, 2, pushedPayload(t, "users")),
	)
	c := newFakeConsumer(t, KafkaConfig{Group: "cdc-go"}, reader)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handled := make(chan string, 4)
	consumed := make(chan error, 1)
	go func() {
		consumed <- c.Consume(ctx, func(_ context.Context, event types.EventData) error {
			handled <- event.Row.Table
			cancel()
			return nil
		})
	}()

	select {
	case table := <-handled:
		assert.Equal(t, "users", table)
	case <-time.After(5 * time.Second):
		t.Fatal("the valid message behind the invalid one was never handled")
	}
	<-consumed

	// A payload that cannot be decoded is dropped for good, otherwise it would be
	// read again on every restart without ever getting further.
	assert.Equal(t, []int64{1, 2}, reader.committedOffsets())
}

func TestKafkaConsumer_Consume_WithoutAGroupCommitsNothing(t *testing.T) {
	reader := newFakeKafkaReader(kafkaMessage(t, 7, pushedPayload(t, "users")))
	c := newFakeConsumer(t, KafkaConfig{}, reader)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handled := make(chan struct{}, 1)
	consumed := make(chan error, 1)
	go func() {
		consumed <- c.Consume(ctx, func(context.Context, types.EventData) error {
			handled <- struct{}{}
			cancel()
			return nil
		})
	}()

	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("the message was never handled")
	}
	<-consumed

	assert.Empty(t, reader.committedOffsets())
}

func TestKafkaConsumer_Consume_ReportsReadErrors(t *testing.T) {
	reader := newFakeKafkaReader()
	reader.readErr = errors.New("the broker is unreachable")
	c := newFakeConsumer(t, KafkaConfig{Group: "cdc-go"}, reader)

	err := c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil })
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read from Kafka")
}

func TestKafkaConsumer_Consume_EOFEndsTheConsumption(t *testing.T) {
	reader := newFakeKafkaReader()
	reader.readErr = io.EOF
	c := newFakeConsumer(t, KafkaConfig{Group: "cdc-go"}, reader)

	assert.NoError(t, c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil }))
}

func TestKafkaConsumer_Consume_ReturnsNilWhenClosed(t *testing.T) {
	reader := newFakeKafkaReader()
	c := newFakeConsumer(t, KafkaConfig{Group: "cdc-go"}, reader)

	consumed := make(chan error, 1)
	go func() {
		consumed <- c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil })
	}()

	// Give the loop a moment to reach the read it will be waiting on.
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, c.Close())

	select {
	case err := <-consumed:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not end the consumption")
	}
}
