package kafka

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const flushTimeout = 500

var errUnknownType = errors.New("unknown error type")

type Producer struct {
	producer *kafka.Producer
}

func NewProducer(addresses []string) (*Producer, error) {
	conf := &kafka.ConfigMap{"bootstrap.servers": strings.Join(addresses, ", ")}

	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, fmt.Errorf("init producer failed %w", err)
	}

	return &Producer{producer: p}, nil

}

func (p *Producer) Produce(message, topic string) error {
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
		// distribution partitioning using a key
		Key:       []byte("test"),
		Timestamp: time.Now(),
	}

	deliveryChan := make(chan kafka.Event)
	if err := p.producer.Produce(kafkaMsg, deliveryChan); err != nil {
		return fmt.Errorf("producer produce failed %w", err)
	}

	e := <-deliveryChan

	switch ev := e.(type) {
	case *kafka.Message:
		return nil
	case kafka.Error:
		return ev
	default:
		return errUnknownType
	}

}

func (p *Producer) Close() {
	p.producer.Flush(flushTimeout)
	p.producer.Close()
}
