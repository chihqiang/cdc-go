package output

import (
	"context"
	"encoding/json"
	"time"

	"github.com/chihqiang/cdc-trigger/types"
	"github.com/segmentio/kafka-go"
)

// KafkaConfig Kafka configuration entity, used to initialize KafkaOutput
type KafkaConfig struct {
	// Brokers List of Kafka brokers, e.g., ["127.0.0.1:9092"]
	Brokers []string `json:"brokers,default=127.0.0.1:9092"`

	// Topic The name of the Kafka topic to send messages to
	Topic string `json:"topic,default=cdc-trigger-events"`
}

// KafkaOutput Kafka implementation that satisfies the IOutput interface
type KafkaOutput struct {
	// writer Kafka writer
	writer *kafka.Writer
	// config Kafka configuration entity
	config KafkaConfig
}

// NewKafkaOutput Creates a KafkaOutput using the configuration entity
// Parameters:
//
//	cfg: KafkaConfig configuration struct, contains broker list and topic name
//
// Returns:
//
//	*KafkaOutput instance
func NewKafkaOutput(cfg KafkaConfig) (*KafkaOutput, error) {
	// Create Kafka writer
	writer := &kafka.Writer{
		// Kafka broker address list
		Addr: kafka.TCP(cfg.Brokers...),
		// Kafka topic to send messages to
		Topic: cfg.Topic,
		// Partition selection strategy, LeastBytes means selecting the partition with the least load
		Balancer: &kafka.LeastBytes{},
		// Wait for all replicas to confirm the message has been written, ensuring message reliability
		RequiredAcks: kafka.RequireAll,
		// Whether the send is asynchronous, false means synchronous sending
		Async: false,
		// Ask the broker to create the topic when it is missing. Without it the
		// metadata request carries allow_auto_topic_creation=false and every
		// send fails with UnknownTopicOrPartition, whatever the broker's
		// auto.create.topics.enable says. The creation is still in flight when
		// the first answer arrives, so that first attempt fails and the retry
		// loop in SendWithRetry is what delivers the event.
		AllowAutoTopicCreation: true,
		// Send is called with a single message at a time, so a batch can never
		// hold more than one: flushing it as soon as it is full writes it right
		// away, where the default BatchTimeout would make every event wait out
		// a whole second.
		BatchSize: 1,
	}
	return &KafkaOutput{
		writer: writer,
		config: cfg,
	}, nil
}

// Send Serializes EventData to a JSON string and sends it to Kafka
// Parameters:
//
//	event: types.EventData the event to send
//
// Returns:
//
//	error error if sending fails, otherwise nil
func (k *KafkaOutput) Send(ctx context.Context, event types.EventData) error {
	// Serialize the event into a JSON string
	eventValue, err := json.Marshal(event)
	if err != nil {
		return err
	}
	// Add a 30s timeout so a broker that is unreachable does not leave
	// the consumer blocked forever: the retry loop in SendWithRetry will
	// retry on this timeout as it would on any other error.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return k.writer.WriteMessages(ctx, kafka.Message{
		Value: eventValue,
		Time:  time.Now(),
	})
}

// Close Closes the Kafka connection
// Returns:
//
//	error error if closing fails, otherwise nil
func (k *KafkaOutput) Close() error {
	return k.writer.Close()
}
