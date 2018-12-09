package consumer

import (
	"fmt"
	"time"
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


type KafkaConsumer struct {
	ConsumeErrors bool
	ConsumeNotifications bool
	OffsetInitial int8
	OffsetCommitInterval time.Duration
	ClusterConsumer *cluster.Consumer
}

func (this *KafkaConsumer) ConsumeErrorsAndNotifications() {
	//Consume errors
	go func() {
		for err := range this.ClusterConsumer.Errors() {
			fmt.Println("消费消息时出错: err = ", err)
		}
	}()

	//consume notifications
	go func() {
		for ntf := range this.ClusterConsumer.Notifications() {
			fmt.Println(ntf)
		}
	}()
}