package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"LogFilter/src/consumer"
	"LogFilter/src/producer"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

var (
	srcTopics = []string{"aixue-qa-logs", "aixue-uat-logs", "aixue-prod-logs"}
	dstTopics = []string{"aixue-qa-logEntry", "aixue-uat-logEntry", "aixue-prod-logEntry"}
)


//从特定的topic消费消息
func ConsumeTopicMessages(c *cluster.Consumer, p sarama.SyncProducer, e <-chan int, d chan<- int) {
	//在函数返回前向任务队列中放一个数，表示该Consumer已经退出
	defer func() {
		fmt.Println("开始执行关闭consumer的任务..")
		d <- 1
		c.Close()
		p.Close()
		fmt.Println("Consumer关闭任务执行完成...")
	}()

	//消费Notification
	go func() {
		for notice := range c.Notifications() {
			log.Println(notice)
		}
	}()

	//消费error
	go func() {
		for err := range c.Errors() {
			log.Printf("Consume error occurred! %v", err)
		}
	}()

	//消费消息
	for {
		select {
		case msg := <-c.Messages():
			log.Printf("Topic = %s Partition = %d Offset = %d msg = %s", msg.Topic, msg.Partition, 
						msg.Offset, string(msg.Value))
			//先确定消息已经消费
			c.MarkOffset(msg, "")
			//再把该消息发送到dstTopic中
			//构造producer消息
			go ProduceMessage(msg, p)
		case <-e:
			fmt.Println("收到退出信息，准备退出>>>")
			return
		}
	}
}

func ProduceMessage(msg *sarama.ConsumerMessage, p sarama.SyncProducer) {
	pmsg := &sarama.ProducerMessage{
		Topic: fmt.Sprintf("%s-pure", msg.Topic),
		Value: sarama.ByteEncoder(msg.Value), //将msg.Value转换成Encoder类型，这样才能当ProducerMessage的value使用
	}
	partition, offset, err := p.SendMessage(pmsg)
	if err != nil {
		log.Println("发送消息到kafka失败， err = ", err)
	} else {
		log.Printf("成功将消息%s 发送到 topic= %s 的 %d 号分区的 %d 位置", string(msg.Value), pmsg.Topic,
					partition, offset)
	}
}



func main(){
	//捕获os.Interrupt信号，在消费时如果捕获到，就正常退出
	//这里会根据srcTopics的数量去启动相应数量的Consumer，所以需要每一个conSumer都需要通知。
	signals := make(chan os.Signal, 1)
	exitChan := make(chan int, len(srcTopics))
	signal.Notify(signals, os.Interrupt)
	

	//创建一个任务chan，当consumer退出的时候向该chan中放一个数
	done := make(chan int, len(srcTopics))

	//创建对应的生产者
	producerList := []sarama.SyncProducer{}
	for _, _ = range dstTopics {
		p := producer.NewKafkaProducer()
		producerList = append(producerList, p)
	}
	log.Println(producerList)

	//从srcTopics中消费消息，
	for i, t := range srcTopics {
		c := consumer.NewKafkaConsumer(fmt.Sprintf("%v-0", t), append([]string{}, t))
		go ConsumeTopicMessages(c, producerList[i], exitChan, done)
	}


	//通过os.Interrupt信号知道要退出程序，然后再通过exitChan来通知Consumer退出
	//由于有多个consumer，所以需要一个一个通知。即不停的向exitChan中发数字
	count := 0
	for {
		select {
		case <-signals:
			fmt.Println("ctrl＋c ...")
			exitChan<-1
		case <-done:
			count++
			if count == len(srcTopics) {
				fmt.Println("所有的Producer和Consumer都已经退出，主程序也退出...")
				return
			}
			exitChan<-1
		}
	}
}