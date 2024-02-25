package hykafka

import (
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

func Dial(host string) (*kafka.Conn, error) {
	conn, err := kafka.Dial("tcp", host)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %w", err)
	}

	return conn, nil
}
