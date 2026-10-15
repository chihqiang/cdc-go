package output

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/chihqiang/cdc-trigger/types"
	"github.com/stretchr/testify/assert"
)

type mockPulsarProducer struct {
	sentMessages [][]byte
	returnError  bool
}

func (m *mockPulsarProducer) Send(ctx context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	if m.returnError {
		return nil, errors.New("mock send error")
	}
	m.sentMessages = append(m.sentMessages, msg.Payload)
	return pulsar.EarliestMessageID(), nil
}

func (m *mockPulsarProducer) SendAsync(ctx context.Context, msg *pulsar.ProducerMessage, cb func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	if m.returnError {
		cb(nil, msg, errors.New("mock send error"))
	} else {
		m.sentMessages = append(m.sentMessages, msg.Payload)
		cb(pulsar.EarliestMessageID(), msg, nil)
	}
}

func (m *mockPulsarProducer) Close()                {}
func (m *mockPulsarProducer) Topic() string         { return "mock-topic" }
func (m *mockPulsarProducer) Name() string          { return "mock-producer" }
func (m *mockPulsarProducer) LastSequenceID() int64 { return 0 }
func (m *mockPulsarProducer) Flush() error          { return nil }
func (m *mockPulsarProducer) FlushWithCtx(ctx context.Context) error {
	return nil
}

// ==== Mock Client ====

type mockClient struct {
	producer pulsar.Producer
}

func (m *mockClient) CreateProducer(opts pulsar.ProducerOptions) (pulsar.Producer, error) {
	return m.producer, nil
}

func (m *mockClient) Close() {}

func (m *mockClient) Subscribe(pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	return nil, errors.New("not implemented")
}

func (m *mockClient) CreateReader(pulsar.ReaderOptions) (pulsar.Reader, error) {
	return nil, errors.New("not implemented")
}

func (m *mockClient) TopicPartitions(topic string) ([]string, error) {
	return []string{topic}, nil
}

func (m *mockClient) CreateTableView(opts pulsar.TableViewOptions) (pulsar.TableView, error) {
	return nil, errors.New("not implemented")
}

func (m *mockClient) NewTransaction(duration time.Duration) (pulsar.Transaction, error) {
	return nil, errors.New("not implemented")
}

// ==== Tests ====
func TestPulsarOutput_Send(t *testing.T) {
	mp := &mockPulsarProducer{}
	mc := &mockClient{producer: mp}

	pout := &PulsarOutput{
		cfg: PulsarConfig{
			URL:   "pulsar://127.0.0.1:6650",
			Topic: "test-topic",
		},
		client:   mc,
		producer: mp,
	}
	pout.dialFn = func() (pulsar.Client, pulsar.Producer, error) {
		return mc, mp, nil
	}

	event := types.EventData{
		Time:     time.Now(),
		ServerID: 101,
		Pos:      202,
		Row: types.EventRowData{
			Time:     time.Now().UnixMilli(),
			Database: "testdb",
			Table:    "users",
			Type:     types.InsertEventRowType,
			Data: map[string]any{
				"id":   1,
				"name": "Bob",
			},
		},
	}

	err := pout.Send(context.Background(), event)
	assert.NoError(t, err)
	assert.Len(t, mp.sentMessages, 1)

	var decoded types.EventData
	err = json.Unmarshal(mp.sentMessages[0], &decoded)
	assert.NoError(t, err)
	assert.Equal(t, event.Row.Table, decoded.Row.Table)
}

func TestPulsarOutput_SendError(t *testing.T) {
	mp := &mockPulsarProducer{returnError: true}
	mc := &mockClient{producer: mp}

	pout := &PulsarOutput{
		cfg:      PulsarConfig{},
		client:   mc,
		producer: mp,
	}
	// Override dial to return the mock instead of trying a real connection.
	pout.dialFn = func() (pulsar.Client, pulsar.Producer, error) {
		return mc, mp, nil
	}

	event := types.EventData{Time: time.Now()}

	err := pout.Send(context.Background(), event)
	assert.Error(t, err)
}

func TestPulsarOutput_Send_InvalidJSON(t *testing.T) {
	mp := &mockPulsarProducer{}
	mc := &mockClient{producer: mp}

	pout := &PulsarOutput{
		cfg:      PulsarConfig{},
		client:   mc,
		producer: mp,
	}
	pout.dialFn = func() (pulsar.Client, pulsar.Producer, error) {
		return mc, mp, nil
	}

	event := types.EventData{
		Time: time.Now(),
		Row: types.EventRowData{
			Data: map[string]any{
				"bad": make(chan int),
			},
		},
	}

	err := pout.Send(context.Background(), event)
	assert.Error(t, err)
	assert.Len(t, mp.sentMessages, 0)
}

func TestPulsarOutput_Close(t *testing.T) {
	mp := &mockPulsarProducer{}
	mc := &mockClient{producer: mp}

	pout := &PulsarOutput{
		cfg:      PulsarConfig{},
		client:   mc,
		producer: mp,
	}
	pout.dialFn = func() (pulsar.Client, pulsar.Producer, error) {
		return mc, mp, nil
	}

	assert.NotPanics(t, func() {
		err := pout.Close()
		assert.NoError(t, err)
	})
}
