package producer

import (
	"fmt"
	// ckafka - apelido da biblioteca importada
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

// Criando o producer
func NewKafkaProducer() *ckafka.Producer {
	configMap := &ckafka.ConfigMap{
		// Quais servidores conectaram com o kagka
		"bootstrap.servers": os.Getenv("kafkaBootstrapServers"),
	}
	// Passar as configuraçãoes para o novo producer
	p, err := ckafka.NewProducer(configMap)
	if err != nil {
		panic(err)
	}
	return p
}

func Publish(msg string, topic string, producer *ckafka.Producer, deliveryChan chan ckafka.Event) error {
	message := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
		// Converte a string para um slice de byte
		Value:          []byte(msg),
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan ckafka.Event) {
	// Um loop para observar todas as mensagens que chegam no canal
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *ckafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Delivery failed:", ev.TopicPartition)
			} else {
				fmt.Println("Delivered message to:", ev.TopicPartition)
			}
		}
	}
}