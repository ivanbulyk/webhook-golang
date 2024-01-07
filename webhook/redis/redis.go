package redis

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"log"
)

// WebhookPayload defines the structure of the data expected
// to be received from Redis, including URL, Webhook ID, and relevant data.
type WebhookPayload struct {
	Url       string `json:"url,omitempty"`
	WebhookId string `json:"webhook_id,omitempty"`

	Data struct {
		Event   string `json:"event,omitempty"`
		Date    string `json:"created"`
		Id      string `json:"id,omitempty"`
		Payment string `json:"payment,omitempty"`
	} `json:"data"`
}

func Subscribe(ctx context.Context, client *redis.Client, webhookQueue chan WebhookPayload) error {
	// Subscribe to webhook channel in Redis
	pubSub := client.Subscribe(ctx, "payments")

	// Ensure that the PubSub connection is closed when the function exits
	defer func(pubSub *redis.PubSub) {
		if err := pubSub.Close(); err != nil {
			log.Println("Error closing PubSub:", err)
		}
	}(pubSub)

	var payload WebhookPayload

	// Infinite loop to continuously receive messages from the "webhooks" channel
	for {
		// Receive a message from the channel
		msg, err := pubSub.ReceiveMessage(ctx)
		if err != nil {
			return err // Return the error if there's an issue receiving the message
		}

		// Unmarshal the JSON payload into the WebhookPayload structure
		err = json.Unmarshal([]byte(msg.Payload), &payload)
		if err != nil {
			log.Println("Error unmarshalling payload:", err)
			continue // Continue with the next message if there's an error unmarshalling
		}

		webhookQueue <- payload // Sending the payload to the channel
	}
}
