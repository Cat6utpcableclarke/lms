package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/Cat6utpcableclarke/API-gateway/graph/model"
	"github.com/coder/websocket"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

const bookServiceURL = "http://localhost:8080/query"
const patronServiceQueue = "patron-service-queue"
const borrowingServiceURL = "http://localhost:8082/query"
const fineServiceURL = "http://fine-service:8000/graphql"

// forwardRequest forwards HTTP requests from the gateway to individual services.
func forwardRequest(ctx context.Context, query string, variables map[string]interface{}, serviceURL string) ([]byte, error) {
	body := map[string]interface{}{
		"query":     query,
		"variables": variables,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", serviceURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json") // Ensure we expect JSON response

	client := &http.Client{
		Timeout: 10 * time.Second, // Add timeout to prevent hanging
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		// Try to parse the error response
		var errResponse struct {
			Errors []struct {
				Message    string `json:"message"`
				Extensions struct {
					Code string `json:"code"`
				} `json:"extensions"`
			} `json:"errors"`
		}

		if err := json.Unmarshal(responseBody, &errResponse); err == nil && len(errResponse.Errors) > 0 {
			if errResponse.Errors[0].Extensions.Code != "" {
				return nil, fmt.Errorf("%s (code: %s)",
					errResponse.Errors[0].Message,
					errResponse.Errors[0].Extensions.Code)
			}
			return nil, fmt.Errorf("%s", errResponse.Errors[0].Message)
		}

		// Fallback to generic error if we couldn't parse the response
		return nil, fmt.Errorf("service returned status %d: %s", resp.StatusCode, string(responseBody))
	}

	return responseBody, nil
}

// forwardRequestMQ forwards requests using RabbitMQ.
func forwardRequestMQ(queue string, variables map[string]interface{}, requestedResolver string) ([]byte, error) {
	conn, err := amqp.Dial("amqps://qyxoakeh:NKblQPKlN6NbvM8yUM22SvBqkDKCop7j@armadillo.rmq.cloudamqp.com/qyxoakeh")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue (%s): %w", queue, err)
	}

	replyQueue, err := ch.QueueDeclare(
		"api-gateway-queue",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare reply queue: %w", err)
	}

	corrID := uuid.New().String()

	body := map[string]interface{}{
		"variables":         variables,
		"requestedResolver": requestedResolver,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	err = ch.Publish(
		"",
		queue,
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: corrID,
			ReplyTo:       replyQueue.Name,
			Body:          jsonBody,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}

	msgs, err := ch.Consume(
		replyQueue.Name,
		"",
		true,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to consume reply: %w", err)
	}

	timeout := time.After(5 * time.Second)
	for {
		select {
		case msg := <-msgs:
			if msg.CorrelationId == corrID {
				return msg.Body, nil
			}
		case <-timeout:
			return nil, fmt.Errorf("timeout waiting for reply from service")
		}
	}
}

// SubscribeToBookAdded subscribes to the bookAdded WebSocket event.
func SubscribeToBookAdded(ctx context.Context, out chan<- *model.Book) error {
	c, _, err := websocket.Dial(ctx, "ws://localhost:8080/query", nil)
	if err != nil {
		return err
	}

	if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"connection_init"}`)); err != nil {
		return err
	}
	_, _, _ = c.Read(ctx)

	payload := map[string]interface{}{
		"id":   "1",
		"type": "start",
		"payload": map[string]interface{}{
			"query": `subscription { bookAdded { id title author_name date_published description image} }`,
		},
	}
	msg, _ := json.Marshal(payload)
	if err := c.Write(ctx, websocket.MessageText, msg); err != nil {
		return err
	}

	go func() {
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				log.Println("WebSocket read failed:", err)
				close(out)
				return
			}

			var msg struct {
				Type    string `json:"type"`
				Payload struct {
					Data struct {
						BookAdded *model.Book `json:"bookAdded"`
					} `json:"data"`
				} `json:"payload"`
			}
			if err := json.Unmarshal(data, &msg); err != nil {
				log.Println("Failed to unmarshal message:", err)
				continue
			}

			if msg.Type == "data" {
				out <- msg.Payload.Data.BookAdded
			}
		}
	}()

	return nil
}
