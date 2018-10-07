package main

import (
	"os"

	"github.com/TerrexTech/go-authserver-query/kafka"
	"github.com/TerrexTech/go-commonutils/commonutil"
)

// initKafkaIOLogin creates a KafkaIO from KafkaAdapter based on set environment variables.
func initKafkaIOLogin() (*kafka.IO, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	consumerGroupName := os.Getenv("KAFKA_CONSUMER_GROUP_LOGIN")
	consumerTopics := os.Getenv("KAFKA_CONSUMER_TOPIC_LOGIN")
	responseTopic := os.Getenv("KAFKA_PRODUCER_TOPIC_LOGIN")

	kafkaAdapter := &kafka.Adapter{
		Brokers:           *commonutil.ParseHosts(brokers),
		ConsumerGroupName: consumerGroupName,
		ConsumerTopics:    *commonutil.ParseHosts(consumerTopics),
		ProducerTopic:     responseTopic,
	}

	return kafkaAdapter.InitIO()
}
