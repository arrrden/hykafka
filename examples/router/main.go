package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	kafka "github.com/arrrden/hykafka"
	"github.com/arrrden/hykafka/examples/router/handlers/listings"
	"github.com/arrrden/hykafka/router"
	"github.com/rs/zerolog"
)

func main() {
	ctx := context.Background()
	logger := zerolog.New(os.Stdout)

	// Attach the Logger to the context.Context
	ctx = logger.WithContext(ctx)
	k, err := kafka.NewKafkaClient(ctx, "localhost:9094")
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
			msg, _ := kafka.NewMessage("beans", "new-listing", listings.NewListingReq{Name: fmt.Sprintf("listing=%d", count)})
			conn.Produce("listing-recommendation-requests", msg)

			count += 1
			time.Sleep(5 * time.Second)
		}
	}()

	rtr := router.NewRouter(ctx, k, &router.NewRouterOptions{
		GroupId:  "boobies",
		ErrTopic: "errors",
	})

	listingsHandler := listings.NewListingsHandler(&listings.Listings{})

	recs := rtr.NewRouteGroup("listing-recommendation-requests", listingsHandler.DefaultHandler)
	recs.HandleMsg("new-listing", listingsHandler.NewListing)
	recs.HandleMsg("updated-listing", listingsHandler.UpdatedListing)

	if err := rtr.Listen(); err != nil {
		log.Fatalf("failed to listen: %v", err.Error())
	}

	fmt.Scanln()
	if err := rtr.Close(); err != nil {
		panic(err)
	}
}
