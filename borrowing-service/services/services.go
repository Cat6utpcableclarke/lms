package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// func GetBookCopyForReturn(bookID string) (*amqp.Connection, string, error) {
// 	// 1. Establish RabbitMQ connection
// 	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
// 	if err != nil {
// 		return nil, "", fmt.Errorf("failed to connect to RabbitMQ: %v", err)
// 	}

// 	// 2. Create a channel
// 	ch, err := conn.Channel()
// 	if err != nil {
// 		conn.Close()
// 		return nil, "", fmt.Errorf("failed to open channel: %v", err)
// 	}
// 	defer ch.Close()

// 	// 3. Declare the response queue
// 	q, err := ch.QueueDeclare(
// 		"",    // name - empty means auto-generate
// 		false, // durable
// 		false, // delete when unused
// 		true,  // exclusive
// 		false, // no-wait
// 		nil,   // arguments
// 	)
// 	if err != nil {
// 		conn.Close()
// 		return nil, "", fmt.Errorf("failed to declare response queue: %v", err)
// 	}

// 	// 4. Create a consumer for the response
// 	msgs, err := ch.Consume(
// 		q.Name, // queue
// 		"",     // consumer
// 		true,   // auto-ack
// 		false,  // exclusive
// 		false,  // no-local
// 		false,  // no-wait
// 		nil,    // args
// 	)
// 	if err != nil {
// 		conn.Close()
// 		return nil, "", fmt.Errorf("failed to register consumer: %v", err)
// 	}

// 	// 5. Create and publish the request
// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()

// 	request := struct {
// 		BookID string `json:"book_id"`
// 		Action string `json:"action"` // "get_copy_for_return"
// 	}{
// 		BookID: bookID,
// 		Action: "get_copy_for_return",
// 	}

// 	body, err := json.Marshal(request)
// 	if err != nil {
// 		conn.Close()
// 		return nil, "", fmt.Errorf("failed to marshal request: %v", err)
// 	}

// 	err = ch.PublishWithContext(ctx,
// 		"",                      // exchange
// 		"book-copies-requests",  // routing key
// 		false,                   // mandatory
// 		false,                   // immediate
// 		amqp.Publishing{
// 			ContentType:   "application/json",
// 			Body:         body,
// 			ReplyTo:      q.Name,
// 			CorrelationId: fmt.Sprintf("%d", time.Now().UnixNano()),
// 		})
// 	if err != nil {
// 		conn.Close()
// 		return nil, "", fmt.Errorf("failed to publish request: %v", err)
// 	}

// 	// 6. Wait for response
// 	select {
// 	case msg := <-msgs:
// 		var response struct {
// 			BookCopyID string `json:"book_copy_id"`
// 			Error     string `json:"error,omitempty"`
// 		}
// 		if err := json.Unmarshal(msg.Body, &response); err != nil {
// 			conn.Close()
// 			return nil, "", fmt.Errorf("failed to unmarshal response: %v", err)
// 		}

// 		if response.Error != "" {
// 			conn.Close()
// 			return nil, "", errors.New(response.Error)
// 		}

// 		if response.BookCopyID == "" {
// 			conn.Close()
// 			return nil, "", errors.New("empty book copy ID received")
// 		}

// 		return conn, response.BookCopyID, nil

// 	case <-ctx.Done():
// 		conn.Close()
// 		return nil, "", errors.New("timeout waiting for book copy response")
// 	}
// }

func CheckAvailability(BookID string) (bool, *amqp.Connection, string, error) {
	conn, err := amqp.Dial("amqps://qyxoakeh:NKblQPKlN6NbvM8yUM22SvBqkDKCop7j@armadillo.rmq.cloudamqp.com/qyxoakeh")
	if err != nil {
		return false, nil, "", fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return false, nil, "", fmt.Errorf("failed to open a channel: %v", err)
	}
	defer ch.Close()

	// Declare the request queue
	_, err = ch.QueueDeclare(
		"bookCopyAvailRequests", // name
		true,                    // durable
		false,                   // delete when unused
		false,                   // exclusive
		false,                   // no-wait
		nil,                     // arguments
	)
	if err != nil {
		conn.Close()
		return false, nil, "", fmt.Errorf("failed to declare the request queue: %v", err)
	}

	// Declare the reply queue
	replyQueue, err := ch.QueueDeclare(
		"bookServiceReplies", // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		conn.Close()
		return false, nil, "", fmt.Errorf("failed to declare the reply queue: %v", err)
	}

	// Send a borrow request
	err = sendAvailabilityRequest(ch, BookID)
	if err != nil {
		conn.Close()
		return false, nil, "", fmt.Errorf("failed to send availability request: %v", err)
	}

	// Consume the reply
	msgs, err := ch.Consume(
		replyQueue.Name, // queue name
		"",              // consumer tag
		true,            // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		conn.Close()
		return false, nil, "", fmt.Errorf("failed to register a consumer for replies: %v", err)
	}

	// Listen for replies
	for d := range msgs {
		log.Printf("Received a reply: %s", d.Body)

		// Parse the reply
		var reply struct {
			BookCopyID string `json:"book_copy_id"`
			Status     string `json:"status"`
		}
		if err := json.Unmarshal(d.Body, &reply); err != nil {
			log.Printf("Failed to parse reply: %v", err)
			continue
		}

		// Check if the book copy is available
		if reply.Status == "Available" {
			log.Printf("Book copy is available. Returning BookCopyID: %s", reply.BookCopyID)
			return true, conn, reply.BookCopyID, nil
		} else {
			log.Printf("No available book copies. Status: %s", reply.Status)
			conn.Close()
			return false, nil, "", nil
		}
	}

	conn.Close()
	return false, nil, "", nil
}

// sendAvailabilityRequest sends a borrow request to check for available book copies
func sendAvailabilityRequest(ch *amqp.Channel, bookID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a JSON payload for the borrow request
	borrowRequest := struct {
		BookID string `json:"book_id"`
	}{
		BookID: bookID,
	}

	requestBody, err := json.Marshal(borrowRequest)
	failOnError(err, "Failed to marshal borrow request payload")

	err = ch.PublishWithContext(ctx,
		"",                      // exchange
		"bookCopyAvailRequests", // routing key
		false,                   // mandatory
		false,                   // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        requestBody,
		})
	failOnError(err, "Failed to publish borrow request")
	log.Printf(" [x] Sent borrow request: %s\n", requestBody)
	return nil
}

// sendUpdateRequest sends a request to update the status of a book copy
func SendUpdateRequest(conn *amqp.Connection, bookCopyID string, status string) error {
	// Validate inputs
	if conn == nil {
		return fmt.Errorf("RabbitMQ connection cannot be nil")
	}
	if bookCopyID == "" {
		return fmt.Errorf("bookCopyID cannot be empty")
	}
	if status == "" {
		return fmt.Errorf("status cannot be empty")
	}

	// Open a new channel with retry logic
	var ch *amqp.Channel
	var err error
	const maxRetries = 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		ch, err = conn.Channel()
		if err == nil {
			break
		}

		if attempt == maxRetries {
			return fmt.Errorf("failed to open channel after %d attempts: %w", maxRetries, err)
		}

		time.Sleep(time.Duration(attempt) * time.Second)
	}
	defer func() {
		if ch != nil {
			ch.Close()
		}
	}()

	// Prepare context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create and marshal payload
	updatePayload := struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}{
		ID:     bookCopyID,
		Status: status,
	}

	updateBody, err := json.Marshal(updatePayload)
	if err != nil {
		return fmt.Errorf("failed to marshal update payload: %w", err)
	}

	// Publish with confirmation
	err = ch.Confirm(false)
	if err != nil {
		return fmt.Errorf("failed to put channel in confirm mode: %w", err)
	}

	confirmation := make(chan amqp.Confirmation, 1)
	ch.NotifyPublish(confirmation)

	err = ch.PublishWithContext(ctx,
		"",                  // exchange
		"book-copies-queue", // routing key
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         updateBody,
			DeliveryMode: amqp.Persistent, // Ensure message survives broker restart
			Timestamp:    time.Now(),
		})

	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	// Wait for confirmation or timeout
	select {
	case confirmed := <-confirmation:
		if !confirmed.Ack {
			return fmt.Errorf("failed to receive ack from broker")
		}
		log.Printf("Successfully published update for BookCopyID %s (status: %s)", bookCopyID, status)
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for publish confirmation: %w", ctx.Err())
	}

	return nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func stringPtr(s string) *string {
	return &s
}

func parseTimePtr(s *string) (*time.Time, error) {
	if s == nil {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, *s)
	return &t, err
}

func GetRabbitMQConnection() (*amqp.Connection, error) {
	conn, err := amqp.Dial("amqps://qyxoakeh:NKblQPKlN6NbvM8yUM22SvBqkDKCop7j@armadillo.rmq.cloudamqp.com/qyxoakeh")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	return conn, nil
}
