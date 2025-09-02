package consumer

import (
	"kafka_go_t/internal/handler"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

const (
	consumerGroup      = "consumerGroup"
	sessionTimeoutMs   = 6000
	autoCommitInterval = 1000
	noTimeOut          = -1
)

type Consumer struct {
	ConsumerNumber int
	consumer       *kafka.Consumer
	handler        *handler.Handler
	stop           bool
}

func NewConsumer(index int, addreses []string, topic string, consumerGroup string, handler *handler.Handler) (*Consumer, error) {
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(addreses, ", "),
		"group.id":                 consumerGroup,
		"session.timeout.ms":       sessionTimeoutMs,
		"enable.auto.offset.store": false,
		"enable.auto.commit":       true,
		"auto.commit.interval.ms":  autoCommitInterval,
		// "auto.offset.reset": "largest",
		"auto.offset.reset": "earliest",
	}
	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, err
	}

	if err := c.Subscribe(topic, nil); err != nil {
		return nil, err
	}

	return &Consumer{ConsumerNumber: index, consumer: c, handler: handler}, nil
}

func (c *Consumer) Start() {
	for {
		if c.stop {
			return
		}

		kafkaMsg, err := c.consumer.ReadMessage(noTimeOut)

		if err != nil {
			logrus.Error("consumer read message err: ", err)
		}

		if kafkaMsg == nil {
			continue
		}

		if err = c.handler.HandleMessage(*kafkaMsg, kafkaMsg.TopicPartition.Offset, c.ConsumerNumber); err != nil {
			logrus.Error("consumer handle message err: ", err)
		}

		if _, err := c.consumer.StoreMessage(kafkaMsg); err != nil {
			logrus.Error("consumer store message err:  ", err)
			continue
		}
	}

}

func (c *Consumer) Stop() error {
	c.stop = true
	if _, err := c.consumer.Commit(); err != nil {
		return err
	}
	return c.consumer.Close()

}
