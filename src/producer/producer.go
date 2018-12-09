package producer

import (
	"fmt"
	"LogFilter/src/config"
	"github.com/Shopify/sarama"
)


func NewKafkaProducer() sarama.SyncProducer {
	producerConfig := config.NewProducerConfig()

	//sarama.NewSyncProducer返回的是一个接口，SyncProducer
	producer, err := sarama.NewSyncProducer(config.BrokerLists, producerConfig)
	if err != nil {
		panic(fmt.Sprintf("创建producer出错，err = %v\n",err))
	}
	return producer
}