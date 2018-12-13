package consumer

import (
	"fmt"
	"LogFilter/src/config"
	cluster "github.com/bsm/sarama-cluster"
)


func NewKafkaConsumer(groupName string, topics []string) *cluster.Consumer {
	//从config包中实例化一个consumer的config
	consumerConfig := config.NewConsumerConfig()

	consumer, err := cluster.NewConsumer(config.BrokerLists, groupName, topics, consumerConfig)
	if err != nil {
		panic(fmt.Sprintf("创建Consumer出错，err = %v\n", err))
	}
	return consumer
}