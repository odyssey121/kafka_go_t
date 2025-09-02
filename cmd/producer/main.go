package main

import (
	"fmt"
	kafka "kafka_go_t/internal/kafka/producer"

	"github.com/sirupsen/logrus"
)

const defaultTopic = "defaultTopic"

var addresses = []string{"localhost:9091", "localhost:9092", "localhost:9093"}

func main() {
	p, err := kafka.NewProducer(addresses)
	if err != nil {
		logrus.Error("init producer err: ", err)
	}

	for i := 0; i < 1000; i++ {
		p.Produce(fmt.Sprintf("message number %d", i+1), defaultTopic)
	}
	p.Close()
}
