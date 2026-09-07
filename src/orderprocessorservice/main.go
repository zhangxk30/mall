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
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultPort        = "8081"
	defaultKafkaTopic  = "order-placed"
	defaultKafkaGroup  = "orderprocessor"
	defaultStatusAfter = "PROCESSED"
)

// kafkaDialer builds a SASL PLAIN dialer for CKafka instances that require
// authentication. Returns nil (default dialer, no auth) when
// KAFKA_USERNAME/KAFKA_PASSWORD aren't set.
func kafkaDialer() *kafka.Dialer {
	username := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")
	if username == "" || password == "" {
		return nil
	}
	return &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: plain.Mechanism{Username: username, Password: password},
	}
}

var (
	log          *logrus.Logger
	extraLatency time.Duration
)

func init() {
	log = logrus.New()
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.Out = os.Stdout
}

// orderPlacedEvent mirrors checkoutservice's order_publisher.go payload.
type orderPlacedEvent struct {
	OrderID            string `json:"order_id"`
	UserID             string `json:"user_id"`
	Email              string `json:"email"`
	TotalCurrencyCode  string `json:"total_currency_code"`
	TotalUnits         int64  `json:"total_units"`
	TotalNanos         int32  `json:"total_nanos"`
	ShippingTrackingID string `json:"shipping_tracking_id"`
}

func main() {
	if os.Getenv("ENABLE_TRACING") == "1" {
		log.Info("Tracing enabled.")
		initTracing()
	} else {
		log.Info("Tracing disabled.")
	}

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{}))

	if s := os.Getenv("EXTRA_LATENCY"); s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			log.Fatalf("failed to parse EXTRA_LATENCY (%s) as time.Duration: %+v", s, err)
		}
		extraLatency = v
		log.Infof("extra latency enabled (duration: %v)", extraLatency)
	}

	store, err := newOrderStore()
	if err != nil {
		log.Fatalf("failed to initialize order store: %+v", err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}
	go startHealthServer(port)

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		log.Fatal("KAFKA_BROKERS not set, orderprocessorservice has nothing to consume")
	}
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = defaultKafkaTopic
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(brokers, ","),
		Topic:   topic,
		GroupID: defaultKafkaGroup,
		Dialer:  kafkaDialer(),
	})
	defer reader.Close()

	log.Infof("consuming from Kafka topic %q as group %q", topic, defaultKafkaGroup)
	consumeLoop(context.Background(), reader, store)
}

func consumeLoop(ctx context.Context, reader *kafka.Reader, store *orderStore) {
	tracer := otel.Tracer("orderprocessorservice")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Errorf("failed to read Kafka message: %+v", err)
			continue
		}
		processMessage(ctx, tracer, store, msg)
	}
}

func processMessage(ctx context.Context, tracer trace.Tracer, store *orderStore, msg kafka.Message) {
	// Extract the traceparent injected by checkoutservice's producer span so
	// this consumer span attaches to the originating PlaceOrder trace instead
	// of starting an orphan trace.
	carrier := &kafkaHeaderCarrier{headers: msg.Headers}
	msgCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)

	// segmentio/kafka-go has no official OTel instrumentation, so the consumer
	// span carries the messaging.* attributes by hand. Partition/offset make
	// consumer lag visible in APM.
	spanCtx, span := tracer.Start(msgCtx,
		fmt.Sprintf("%s process", msg.Topic),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(msg.Topic),
			semconv.MessagingOperationTypeProcess,
			semconv.MessagingKafkaOffset(int(msg.Offset)),
			semconv.MessagingDestinationPartitionID(strconv.Itoa(msg.Partition)),
			semconv.MessagingMessageBodySize(len(msg.Value)),
		))
	defer span.End()

	var event orderPlacedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Errorf("failed to unmarshal order-placed event: %+v", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to unmarshal order-placed event")
		return
	}
	span.SetAttributes(semconv.MessagingKafkaMessageKey(event.OrderID))
	log.Infof("[processOrder] order_id=%q", event.OrderID)

	if extraLatency > 0 {
		time.Sleep(extraLatency)
	}

	if err := store.UpdateOrderStatus(spanCtx, event.OrderID, defaultStatusAfter); err != nil {
		log.Warnf("failed to update order status for %q: %+v", event.OrderID, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to update order status")
		return
	}
	log.Infof("[processOrder] order_id=%q status=%s", event.OrderID, defaultStatusAfter)
}

func startHealthServer(port string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})
	log.Infof("starting health check server at :%s", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%s", port), mux); err != nil {
		log.Fatalf("health server failed: %+v", err)
	}
}

func initTracing() {
	ctx := context.Background()

	var endpoint, token, serviceName, hostName string
	mustMapEnv(&endpoint, "APM_ENDPOINT")
	mustMapEnv(&token, "APM_TOKEN")
	mustMapEnv(&serviceName, "APM_SERVICE_NAME")
	mustMapEnv(&hostName, "APM_HOST_NAME")

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure())
	if err != nil {
		log.Warnf("warn: Failed to create trace exporter: %v", err)
		return
	}

	r, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("token", token),
		attribute.String("service.name", serviceName),
		attribute.String("host.name", hostName),
	))
	if err != nil {
		log.Warnf("warn: Failed to create resource: %v", err)
		return
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(r))
	otel.SetTracerProvider(tp)
}

func mustMapEnv(target *string, envKey string) {
	v := os.Getenv(envKey)
	if v == "" {
		panic(fmt.Sprintf("environment variable %q not set", envKey))
	}
	*target = v
}

// kafkaHeaderCarrier adapts []kafka.Header to otel's propagation.TextMapCarrier,
// mirroring checkoutservice/order_publisher.go's injecting counterpart.
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
