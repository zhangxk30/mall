// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/checkoutservice/genproto"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultOrderPlacedTopic = "order-placed"
	tracerName              = "checkoutservice"
)

// kafkaTransport builds a SASL PLAIN transport for CKafka instances that
// require authentication. Returns nil (a true nil kafka.RoundTripper, so
// kafka.Writer falls back to its DefaultTransport) when
// KAFKA_USERNAME/KAFKA_PASSWORD aren't set. Returning a nil *kafka.Transport
// here instead would produce a non-nil interface wrapping a nil pointer,
// which panics inside kafka.Writer.WriteMessages.
func kafkaTransport() kafka.RoundTripper {
	username := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")
	if username == "" || password == "" {
		return nil
	}
	return &kafka.Transport{
		SASL: plain.Mechanism{Username: username, Password: password},
	}
}

// orderPlacedEvent is the JSON body published to Kafka. JSON (rather than
// protobuf) keeps the message human-readable in the CKafka console.
type orderPlacedEvent struct {
	OrderID            string `json:"order_id"`
	UserID             string `json:"user_id"`
	Email              string `json:"email"`
	TotalCurrencyCode  string `json:"total_currency_code"`
	TotalUnits         int64  `json:"total_units"`
	TotalNanos         int32  `json:"total_nanos"`
	ShippingTrackingID string `json:"shipping_tracking_id"`
}

// kafkaHeaderCarrier adapts []kafka.Header to otel's propagation.TextMapCarrier
// so trace context can be injected into / extracted from Kafka message headers.
type kafkaHeaderCarrier struct {
	headers []kafka.Header
}

func (c *kafkaHeaderCarrier) Get(key string) string {
	for _, h := range c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *kafkaHeaderCarrier) Set(key, value string) {
	for i, h := range c.headers {
		if h.Key == key {
			c.headers[i].Value = []byte(value)
			return
		}
	}
	c.headers = append(c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c *kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(c.headers))
	for i, h := range c.headers {
		keys[i] = h.Key
	}
	return keys
}

// orderPublisher publishes order-placed events to Kafka. A nil writer means
// Kafka wasn't configured; PublishOrderPlaced becomes a no-op.
type orderPublisher struct {
	writer *kafka.Writer
	topic  string
}

func newOrderPublisher() *orderPublisher {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		log.Info("KAFKA_BROKERS not set, order-placed events will not be published")
		return &orderPublisher{}
	}

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = defaultOrderPlacedTopic
	}

	writer := &kafka.Writer{
		Addr:      kafka.TCP(strings.Split(brokers, ",")...),
		Topic:     topic,
		Balancer:  &kafka.Hash{},
		Transport: kafkaTransport(),
	}

	log.Infof("publishing order-placed events to Kafka topic %q", topic)
	return &orderPublisher{writer: writer, topic: topic}
}

// PublishOrderPlaced publishes the order and injects the current trace
// context into the message headers so orderprocessorservice's consumer span
// links back to this request's trace.
func (p *orderPublisher) PublishOrderPlaced(ctx context.Context, order *pb.OrderResult, userID, email string, total *pb.Money) error {
	if p == nil || p.writer == nil {
		return nil
	}

	event := orderPlacedEvent{
		OrderID:            order.GetOrderId(),
		UserID:             userID,
		Email:              email,
		TotalCurrencyCode:  total.GetCurrencyCode(),
		TotalUnits:         total.GetUnits(),
		TotalNanos:         total.GetNanos(),
		ShippingTrackingID: order.GetShippingTrackingId(),
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal order-placed event: %w", err)
	}

	// segmentio/kafka-go has no official OTel instrumentation, so the producer
	// span is created by hand. It must be started BEFORE Inject and the
	// returned ctx must be the one injected, otherwise the message carries the
	// parent (PlaceOrder) span and the consumer attaches to the wrong node.
	ctx, span := otel.Tracer(tracerName).Start(ctx,
		fmt.Sprintf("%s publish", p.topic),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(p.topic),
			semconv.MessagingOperationTypePublish,
			semconv.MessagingMessageBodySize(len(body)),
			semconv.MessagingKafkaMessageKey(order.GetOrderId()),
		))
	defer span.End()

	carrier := &kafkaHeaderCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	msg := kafka.Message{
		Key:     []byte(order.GetOrderId()),
		Value:   body,
		Headers: carrier.headers,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to publish order-placed event")
		return fmt.Errorf("failed to publish order-placed event: %w", err)
	}
	return nil
}
