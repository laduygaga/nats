package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	// connect to nats server
	nc, err := nats.Connect("connect.ngs.global", nats.UserCredentials("/home/duy/nats.creds"), nats.Name("NATS JetStream Sample Publisher"))
	// nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	// create jetstream context from nats connection
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	// producer
	go producer(ctx, js)
	// consummer
	go consummer(ctx, js)
	// wait for interrupt signal to exit
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
}
	
func producer(ctx context.Context, js jetstream.JetStream) {
	// create stream
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "ORDERS",
		Description: "ORDERS",
		Subjects: []string{"ORDERS.*"},
		MaxBytes: 1024 * 1024 * 1024,
	})

	if err != nil {
		log.Fatal(err)
	}
	
	i := 0
	for {
		i++
		_, err := js.Publish(ctx, fmt.Sprintf("ORDERS.%d", i), []byte(fmt.Sprintf("ORDERS.%d", i)))
		if err != nil {
			log.Println(err)
			continue
		}
		log.Println("Published message", i)
	}
}


func consummer(ctx context.Context, js jetstream.JetStream) {
	stream, err := js.Stream(ctx, "ORDERS")
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:		"ORDERS",
		Durable:        "ORDERS",
	})
	if err != nil {
		log.Fatal(err)
	}

	cctx, err := consumer.Consume(func(m jetstream.Msg) {
		log.Printf("Received message %s\n", string(m.Subject()))
		m.Ack()
	})

	if err != nil {
		log.Fatal(err)
	}
	
	// gracefully shutdown
	defer cctx.Stop()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
}

