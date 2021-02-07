package consumer

import (
	"fmt"
	kafka "github.com/andressandrade/imersao-fullstack-fullcycle/codepix/producer"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

type KafkaProcessor struct {
	Producer     *ckafka.Producer
	DeliveryChan chan ckafka.Event
}

func NewKafkaProcessor(producer *ckafka.Producer, deliveryChan chan ckafka.Event) *KafkaProcessor {
	return &KafkaProcessor{
		Producer:     producer,
		DeliveryChan: deliveryChan,
	}
}

func (k *KafkaProcessor) Consume() {
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": os.Getenv("kafkaBootstrapServers"),
		"group.id":          os.Getenv("kafkaConsumerGroupId"),
		"auto.offset.reset": "earliest",
	}
	c, err := ckafka.NewConsumer(configMap)

	if err != nil {
		panic(err)
	}

	topics := []string{"desafio2"}
	c.SubscribeTopics(topics, nil)

	fmt.Println("kafka consumer has been started")
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Println(string(msg.Value))
			k.processMessage(msg)
		}
	}
}

func (k *KafkaProcessor) processMessage(msg *ckafka.Message) {

	kafka.Publish(string("Mensagem recebida"), "desafio2", k.Producer, k.DeliveryChan)
	kafka.DeliveryReport(k.DeliveryChan)
	// if err != nil {
	// 	return err
	// }
	// return nil


}


