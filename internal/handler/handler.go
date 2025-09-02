package handler

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

type Handler struct {
}

func (h *Handler) HandleMessage(msg kafka.Message, offset kafka.Offset, consumerNumber int) error {
	logrus.Infof("consumer with number = %d, handler recieve message: %s, offeset %d", consumerNumber, string(msg.Value), offset)
	return nil
}

func NewHandler() *Handler {
	h := &Handler{}
	return h
}
