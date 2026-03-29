package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var kafkaBrokers []string

func main() {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	kafkaBrokers = strings.Split(brokersEnv, ",")

	// Start consumers in background
	go consumeTopic("movie-events")
	go consumeTopic("user-events")
	go consumeTopic("payment-events")

	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", movieEventHandler)
	http.HandleFunc("/api/events/user", userEventHandler)
	http.HandleFunc("/api/events/payment", paymentEventHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Printf("Starting events service on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

func movieEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	movieID := fmt.Sprintf("%v", payload["movie_id"])
	action := fmt.Sprintf("%v", payload["action"])
	eventID := fmt.Sprintf("movie-%s-%s", movieID, action)

	event := Event{
		ID:        eventID,
		Type:      "movie",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payload,
	}

	partition, offset, err := publishEvent("movie-events", event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func userEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	userID := fmt.Sprintf("%v", payload["user_id"])
	action := fmt.Sprintf("%v", payload["action"])
	eventID := fmt.Sprintf("user-%s-%s", userID, action)

	event := Event{
		ID:        eventID,
		Type:      "user",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payload,
	}

	partition, offset, err := publishEvent("user-events", event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func paymentEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	paymentID := fmt.Sprintf("%v", payload["payment_id"])
	status := fmt.Sprintf("%v", payload["status"])
	eventID := fmt.Sprintf("payment-%s-%s", paymentID, status)

	event := Event{
		ID:        eventID,
		Type:      "payment",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payload,
	}

	partition, offset, err := publishEvent("payment-events", event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func publishEvent(topic string, event Event) (int, int64, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return 0, 0, err
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBrokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
	}
	defer writer.Close()

	msg := kafka.Message{
		Key:   []byte(event.ID),
		Value: data,
	}

	err = writer.WriteMessages(context.Background(), msg)
	if err != nil {
		log.Printf("Failed to publish event to %s: %v", topic, err)
		return 0, 0, err
	}

	log.Printf("Published event to %s: %s", topic, event.ID)
	return 0, 0, nil
}

func consumeTopic(topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        kafkaBrokers,
		Topic:          topic,
		GroupID:        "events-service-group",
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	defer reader.Close()

	log.Printf("Started consumer for topic: %s", topic)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading from %s: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}

		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error unmarshalling message from %s: %v", topic, err)
			continue
		}

		log.Printf("[%s] Consumed event: id=%s type=%s timestamp=%s payload=%v",
			topic, event.ID, event.Type, event.Timestamp, event.Payload)
	}
}
