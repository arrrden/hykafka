package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/arrrden/hykafka/hykafka"
	gkafka "github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()
	k, err := kafka.NewKafkaClient(ctx, "localhost:9094", nil)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := k.NewConnection()
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	go func() {
		count := 0
		for {
			conn.Produce("listing-recommendation-requests", gkafka.Message{
				Key:   []byte("beans"),
				Value: []byte(fmt.Sprintf("boobies %d", count)),
			})

			count += 1
			time.Sleep(2 * time.Second)
		}
	}()

	messageCh := make(chan gkafka.Message)
	errorCh := make(chan error)
	quit := make(chan struct{}, 1)

	subscribed := true

	go func() {
		<-quit
		subscribed = false
	}()

	go func() {
		conn.Subscribe([]string{"listing-recommendation-requests"}, "boobies", messageCh, errorCh, &subscribed)
	}()

	go func() {
	loop:
		for {
			if !subscribed {
				break loop
			}

			select {
			case msg := <-messageCh:
				fmt.Printf("received message: %+v\n\n", string(msg.Value))
			case errs := <-errorCh:
				if errs != nil {
					fmt.Printf("received error: %s\n\n", errs.Error())
				}
			}
		}
	}()

	fmt.Scanln()

	close(quit)

	fmt.Scanln()
}
