package hykafka

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

type Message = kafka.Message

func NewMessage(key, msgName string, value interface{}) (Message, error) {
	var message bytes.Buffer
	enc := gob.NewEncoder(&message)

	msg := kafka.Message{
		Key: []byte(key),
		Headers: []protocol.Header{
			{
				Key:   MsgHeaderMessageName,
				Value: []byte(msgName),
			},
		},
	}

	if err := enc.Encode(value); err != nil {
		return msg, fmt.Errorf("failed to encode message: %w", err)
	}

	msg.Value = message.Bytes()

	return msg, nil
}

func DecodeMessage[T interface{}](data []byte, res T) (*T, error) {
	outputBuffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(outputBuffer)

	msg := res

	if err := dec.Decode(&msg); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			fmt.Println("failed to decode message value: ", err)
			return nil, err
		}
	}

	return &msg, nil
}
