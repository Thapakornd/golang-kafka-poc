package services

import (
	"context"
	"encoding/json"
	"example/com/m/v2/model"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaService struct {
	Producer *kafka.Producer
	Consumer *kafka.Consumer
}

func NewKafkaService() *KafkaService {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_SERVERS"),
		"client.id":         os.Getenv("KAFKA_CLIENT_ID"),
		"acks":              "all",
	})
	if err != nil {
		log.Printf("error kafka producer: %s\n", err.Error())
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_SERVERS"),
		"group.id":          os.Getenv("KAFKA_GROUP_ID"),
		"auto.offset.reset": "smallest",
	})
	if err != nil {
		log.Printf("error kafka consumer: %s\n", err.Error())
	}

	return &KafkaService{
		Producer: p,
		Consumer: c,
	}
}

func (ks *KafkaService) ProduceMessage(ctx context.Context, topicPar *kafka.TopicPartition, value *model.ProduceMessage) error {
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if err := ks.Producer.Produce(&kafka.Message{
		TopicPartition: *topicPar,
		Value:          valueJSON},
		nil,
	); err != nil {
		return err
	}

	return nil
}

func (ks *KafkaService) ConsumeMessage(ctx context.Context, topics []string) error {
	err := ks.Consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return err
	}

	for {
		ev := ks.Consumer.Poll(-1)

		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("consumer already ")
		}
	}
}
