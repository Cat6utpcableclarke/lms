package consumer

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Cat6utpcableclarke/bookService/graph"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	rabbitMQURL     = "amqps://qyxoakeh:NKblQPKlN6NbvM8yUM22SvBqkDKCop7j@armadillo.rmq.cloudamqp.com/qyxoakeh"
	bookCopiesQueue = "book-copies-queue"
)

func UpdateConsumer(resolver *graph.Resolver) {
	conn, err := amqp.Dial(rabbitMQURL)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	_, err = ch.QueueDeclare(
		bookCopiesQueue, // queue name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		bookCopiesQueue, // queue name
		"",              // consumer tag
		true,            // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			var payload struct {
				ID     string  `json:"id"`
				Status *string `json:"status"`
			}
			if err := json.Unmarshal(d.Body, &payload); err != nil {
				log.Printf("Failed to parse message: %v", err)
				continue
			}

			//Call the UpdateBookCopyStatus resolver
			ctx := context.Background()
			updatedCopy, err := resolver.Mutation().UpdateBookCopyStatus(ctx, payload.ID, payload.Status)
			if err != nil {
				log.Printf("Failed to update book copy status: %v", err)
				continue
			}

			log.Printf("Successfully updated book copy: %+v", updatedCopy)
		}
	}()

	log.Println("Waiting for messages. To exit press CTRL+C")
	select {}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func ListenAvailabiltyRequests(resolver *graph.Resolver) {
	conn, err := amqp.Dial(rabbitMQURL)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Listen for availability requests
	_, err = ch.QueueDeclare(
		"bookCopyAvailRequests", // queue name
		true,                    // durable
		false,                   // delete when unused
		false,                   // exclusive
		false,                   // no-wait
		nil,                     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Declare the reply queue
	_, err = ch.QueueDeclare(
		"bookServiceReplies", // reply queue name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	failOnError(err, "Failed to declare the reply queue")

	msgs, err := ch.Consume(
		"bookCopyAvailRequests", // queue name
		"",                      // consumer tag
		true,                    // auto-ack
		false,                   // exclusive
		false,                   // no-local
		false,                   // no-wait
		nil,                     // arguments
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)

			// Parse the incoming message
			var payload struct {
				BookID string `json:"book_id"`
			}
			if err := json.Unmarshal(d.Body, &payload); err != nil {
				log.Printf("Failed to parse message: %v", err)
				continue
			}

			// Call the GetAvailbleBookCopyByID resolver
			ctx := context.Background()
			bookCopy, err := resolver.Query().GetAvailbleBookCopyByID(ctx, payload.BookID)
			var replyPayload struct {
				BookCopyID string `json:"book_copy_id"`
				Status     string `json:"status"`
			}

			if bookCopy == nil || err != nil {
				// Handle the case where the resolver returns nil
				log.Printf("No available book copy found for BookID: %s", payload.BookID)
				replyPayload = struct {
					BookCopyID string `json:"book_copy_id"`
					Status     string `json:"status"`
				}{
					BookCopyID: "0",
					Status:     "Not Available",
				}
			} else {
				// Prepare the reply payload for an available book copy
				replyPayload = struct {
					BookCopyID string `json:"book_copy_id"`
					Status     string `json:"status"`
				}{
					BookCopyID: bookCopy.ID,
					Status:     bookCopy.BookStatus,
				}
			}

			// Marshal the reply payload to JSON
			replyBody, err := json.Marshal(replyPayload)
			if err != nil {
				log.Printf("Failed to marshal reply payload: %v", err)
				continue
			}

			// Publish the reply to the borrowing service
			log.Printf("Publishing reply: %s", replyBody)
			err = ch.Publish(
				"",                   // exchange
				"bookServiceReplies", // routing key
				false,                // mandatory
				false,                // immediate
				amqp.Publishing{
					ContentType: "application/json",
					Body:        replyBody,
				},
			)
			if err != nil {
				log.Printf("Failed to publish reply: %v", err)
				continue
			}

			log.Printf("Successfully sent reply: %s", replyBody)
		}
	}()

	log.Println("Listening for availability requests. To exit press CTRL+C")
	select {}
}
