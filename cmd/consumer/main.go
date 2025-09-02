package main

import (
	"fmt"
	"kafka_go_t/internal/handler"
	kafka "kafka_go_t/internal/kafka/consumer"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
)

const (
	defaultTopic    = "defaultTopic"
	consumerGroup   = "consumerGroup"
	consumerNumbers = 3
)

var addresses = []string{"localhost:9091", "localhost:9092", "localhost:9093"}

func main() {
	consumersCh := make(chan kafka.Consumer, consumerNumbers)
	handler := handler.NewHandler()
	for i := 1; i <= consumerNumbers; i++ {
		go func(index int) {
			consumer, err := kafka.NewConsumer(index, addresses, defaultTopic, fmt.Sprintf("%s_%d", consumerGroup, index), handler)
			if err != nil {
				logrus.Fatal(fmt.Sprintf("init consumer with number = %d, error: %v", index, err))
			}

			consumersCh <- *consumer
			logrus.Infof("consumer with number = %d started!", index)
			consumer.Start()

		}(i)

	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh
	for i := 1; i <= consumerNumbers; i++ {
		c := <-consumersCh
		logrus.Infof("consumer with number = %d stoped!", c.ConsumerNumber)
		logrus.Error(c.Stop())
	}

}
