package consumer

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
	"bytes"
	"github.com/streadway/amqp"
)

type ReturnEvent struct {
	PatronID   string `json:"patron_id"`
	BookID     string `json:"book_id"`
	ReturnedAt string `json:"returned_at"`
}

func ConsumeReturnEvent() error {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqps://qyxoakeh:NKblQPKlN6NbvM8yUM22SvBqkDKCop7j@armadillo.rmq.cloudamqp.com/qyxoakeh")
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	// Open a channel
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	// Declare the queue (borrowing.returned) to consume from
	_, err = ch.QueueDeclare(
		"borrowing.returned", // queue name
		true,                 // durable
		false,                // auto-delete
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Consume messages from the queue
	msgs, err := ch.Consume(
		"borrowing.returned", // queue name
		"",                   // consumer tag
		true,                 // auto-ack
		false,                // exclusive
		false,                // no-local
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	// Start listening for messages
	for d := range msgs {
		var event ReturnEvent
		err := json.Unmarshal(d.Body, &event)
		if err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			continue
		}

		// Calculate late days and create fine
		daysLate, err := calculateLateDays(event.ReturnedAt)
		if err != nil {
			log.Printf("Error calculating late days: %v", err)
			continue
		}

		// Create the fine (no need to use GraphQL mutation here yet)
		err = createFine(event.PatronID, event.BookID, 1.0, daysLate) // 1.0 is the rate per day, adjust as needed
		if err != nil {
			log.Printf("Error creating fine: %v", err)
			continue
		}

		// Optionally, notify via RabbitMQ or log the event here
	}
	return nil
}

// Calculate late days based on the returned date and due date
func calculateLateDays(returnedAt string) (int, error) {
	// Parse the returned date
	parsedReturnedAt, err := time.Parse(time.RFC3339, returnedAt)
	if err != nil {
		return 0, fmt.Errorf("invalid returnedAt date format: %w", err)
	}

	// Get the current date (or you could use due date if needed)
	now := time.Now()

	// Calculate days late
	if parsedReturnedAt.After(now) {
		return 0, nil // Book is not late
	}
	daysLate := int(now.Sub(parsedReturnedAt).Hours() / 24)

	return daysLate, nil
}

func createFine(patronID, bookID string, ratePerDay float64, daysLate int) error {
	mutation := `
		mutation CreateFine($patronId: ID!, $bookId: ID!, $ratePerDay: Float!) {
			createFine(patronId: $patronId, bookId: $bookId, ratePerDay: $ratePerDay) {
				fineId
				amount
			}
		}
	`

	variables := map[string]interface{}{
		"patronId":   patronID,
		"bookId":     bookID,
		"ratePerDay": ratePerDay * float64(daysLate), // total fine
	}

	payload := map[string]interface{}{
		"query":     mutation,
		"variables": variables,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal GraphQL request: %w", err)
	}

	req, err := http.NewRequest("POST", "http://localhost:8000/graphql", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("GraphQL request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GraphQL returned non-OK status: %s", resp.Status)
	}

	return nil
}

