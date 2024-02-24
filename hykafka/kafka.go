package hykafka

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	kafka "github.com/segmentio/kafka-go"
)

type KafkaClient struct {
	ctx    context.Context
	Host   string
	Topics map[string]int
	logger zerolog.Logger
	log    func(*zerolog.Event)
}

func NewKafkaClient(ctx context.Context, host string, logger *zerolog.Logger) (*KafkaClient, error) {
	// dial the client to ensure it's valid
	conn, err := Dial(host)
	if err != nil {
		return nil, err
	}

	conn.Close()

	client := &KafkaClient{
		ctx:    ctx,
		Host:   host,
		logger: *logger,
	}
	client.log = func(e *zerolog.Event) {
		if logger != nil {
			e.Send()
		}
	}

	return client, nil
}

func (c *KafkaClient) NewConnection() (*Connection, error) {
	conn := &Connection{}

	kconn, err := Dial(c.Host)
	if err != nil {
		return nil, err
	}

	conn.Conn = kconn
	conn.Client = c
	conn.Retries = 5
	return conn, nil
}

func (c *KafkaClient) GetTopics(conn *kafka.Conn) error {
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %w", err)
	}

	c.Topics = map[string]int{}

	for _, p := range partitions {
		c.Topics[p.Topic] = p.ID
	}

	return nil
}
