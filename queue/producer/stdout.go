package producer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/chihqiang/cdc-trigger/types"
)

// StdoutProducer Console producer implementation
type StdoutProducer struct{}

// NewStdoutProducer Creates a StdoutProducer instance
func NewStdoutProducer() (*StdoutProducer, error) {
	return &StdoutProducer{}, nil
}

// Send Produces the event to the console
func (s *StdoutProducer) Send(ctx context.Context, event types.EventData) error {
	data, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	fmt.Println(string(data))
	return nil
}

// Close No resources to close for console producer
func (s *StdoutProducer) Close() error {
	return nil
}
