package main

import (
	"context"
	"fmt"
	"kafka_go_t/internal/handler"
	kafka "kafka_go_t/internal/kafka/consumer"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	defaultTopic    = "defaultTopic"
	consumerGroup   = "consumerGroup"
	consumerNumbers = 10
)

var addresses = []string{"localhost:9091", "localhost:9092", "localhost:9093"}

var interruptSignals = []os.Signal{
	os.Interrupt,
	syscall.SIGTERM,
	syscall.SIGINT,
}

func main() {

	ctx, stop := signal.NotifyContext(context.Background(), interruptSignals...)
	errGroup, ctx := errgroup.WithContext(ctx)
	defer stop()

	consumersCh := make(chan *kafka.Consumer, consumerNumbers)
	handler := handler.NewHandler()
	for i := 1; i <= consumerNumbers; i++ {
		i := i
		errGroup.Go(func() error {
			consumer, err := kafka.NewConsumer(i, addresses, defaultTopic, fmt.Sprintf("%s_%d", consumerGroup, i), handler)
			if err != nil {
				logrus.Fatal(fmt.Sprintf("init consumer with number = %d, error: %v", i, err))
				return err
			}

			consumersCh <- consumer
			logrus.Infof("consumer with number = %d started!", consumer.ConsumerNumber)
			consumer.Start()
			return nil
		})

	}

	errGroup.Go(func() error {
		<-ctx.Done()
		for i := 1; i <= consumerNumbers; i++ {
			c := <-consumersCh
			logrus.Infof("consumer with number = %d stoped!", c.ConsumerNumber)
			err := c.Stop()
			if err != nil {
				logrus.Infof("stop error: %v", err)
			}
		}
		close(consumersCh)
		return nil
	})

	if err := errGroup.Wait(); err != nil {
		logrus.Fatal(err)
	}
}
