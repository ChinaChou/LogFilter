package main

import (
	"LogFilter/src/config"
	"LogFilter/src/consumer"
	"LogFilter/src/producer"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/go-redis/redis"
)

//filebeat发往kafka的消息结构，其中不需要的字段都使用空接口来接收，在反序列化的时候会自动的对其进行转换
type KafkaMessage struct {
	Timestamp  interface{}       `json:"@timestamp"`
	Metadata   interface{}       `json:"@metadata"`
	Source     interface{}       `json:"source"`
	Fields     map[string]string `json:"fields"`
	Host       interface{}       `json:"host"`
	Offset     int               `json:"offset"`
	Stream     interface{}       `json:"stream"`
	Message    string            `json:"message"`
	Prospector interface{}       `json:"prospector"`
	Input      interface{}       `json:"input"`
	Kubernetes K8sFileds         `json:"kubernetes"`
	Beat       interface{}       `json:"beat"`
}

type K8sFileds struct {
	NameSpace  string            `json:"namespace"`
	Container  map[string]string `json:"container"`
	Replicaset map[string]string `json:"replicaset"`
	Labels     map[string]string `json:"labels"`
	Pod        map[string]string `json:"pod"`
	Node       map[string]string `json:"node"`
}

var (
	regexpList  []*regexp.Regexp
	redisClient *redis.Client
)

func main() {
	for _, v := range config.FilterRules {
		regexpList = append(regexpList, regexp.MustCompile(v))
	}
	//初始化redisClient
	redisClient = redis.NewClient(&redis.Options{
		Addr:     config.RedisHostAndPort,
		Password: config.RedisPassword,
		DB:       config.RedisDb,
	})
	defer redisClient.Close()

	//捕获os.Interrupt信号，在消费时如果捕获到，就正常退出
	//这里会根据srcTopics的数量去启动相应数量的Consumer，所以需要每一个conSumer都需要通知。
	signals := make(chan os.Signal, 1)
	exitChan := make(chan int, len(config.SrcTopics))
	signal.Notify(signals, os.Interrupt)

	//创建一个任务chan，当consumer退出的时候向该chan中放一个数
	done := make(chan int, len(config.SrcTopics))

	//创建对应的生产者
	producerList := []sarama.SyncProducer{}
	for _, _ = range config.SrcTopics {
		p := producer.NewKafkaProducer()
		producerList = append(producerList, p)
	}

	//从srcTopics中消费消息，
	for i, t := range config.SrcTopics {
		c := consumer.NewKafkaConsumer(config.KafkaGroupName, append([]string{}, t))
		go ConsumeTopicMessages(c, producerList[i], exitChan, done, redisClient)
	}

	//通过os.Interrupt信号知道要退出程序，然后再通过exitChan来通知Consumer退出
	//由于有多个consumer，所以需要一个一个通知。即不停的向exitChan中发数字
	count := 0
	for {
		select {
		case <-signals:
			// log.Println("ctrl＋c ...")
			exitChan <- 1
		case <-done:
			count++
			if count == len(config.SrcTopics) {
				log.Println("所有的Producer和Consumer都已经退出，主程序也退出...")
				return
			}
			exitChan <- 1
		}
	}
}

//从特定的topic消费消息
func ConsumeTopicMessages(c *cluster.Consumer, p sarama.SyncProducer, e <-chan int, d chan<- int, redisClient *redis.Client) {
	//在函数返回前向任务队列中放一个数，表示该Consumer已经退出
	defer func() {
		log.Println("开始执行关闭consumer的任务...")
		c.Close()
		p.Close()
		log.Println("Consumer关闭任务执行完成...")
		d <- 1
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
			// log.Printf("Topic = %s Partition = %d Offset = %d msg = %s", msg.Topic, msg.Partition,
			// msg.Offset, string(msg.Value))
			//先确定消息已经消费
			c.MarkOffset(msg, "")

			//反序列化消息
			logEntry := &KafkaMessage{}
			err := json.Unmarshal(msg.Value, logEntry)
			if err != nil {
				log.Printf("反序列化消息失败, 消息＝%v\terr=%v\n", string(msg.Value), err)
				continue
			}

			//然后对消息进行过滤
			// 1、丢掉包含 127.0.0.1及其他 的无用访问日志
			// 2、挑选出带有err, error的日志，并将其发送到redis中；
			if idx, res := Filter(logEntry); res {
				if idx < config.AlertRuleCount {
					redisClient.LPush(config.MsgKey, msg.Value) //将msg.Value存入redis,这样便于alert取到错误日志的更多信息
					//将错误消息重新放入队列
					go ProduceMessage(msg, p)
				}
			} else {
				//未匹配，再把该消息发送到dstTopic中
				go ProduceMessage(msg, p)
			}

		case <-e:
			log.Println("收到退出信息，准备退出>>>")
			return
		}
	}
}

//重新将日志发送到目标topic中供logstash消费存入elasticsearch中
func ProduceMessage(msg *sarama.ConsumerMessage, p sarama.SyncProducer) {
	pmsg := &sarama.ProducerMessage{
		Topic: fmt.Sprintf("%s-%s", msg.Topic, config.DstTopicSuffix),
		Value: sarama.ByteEncoder(msg.Value), //将msg.Value转换成Encoder类型，这样才能当ProducerMessage的value使用
	}
	_, _, err := p.SendMessage(pmsg)
	if err != nil {
		log.Println("发送消息到kafka失败， err = ", err)
	}
}

//过虑日志
func Filter(m *KafkaMessage) (pos int, res bool) {
	//遍历正则列表，对消息进行匹配，匹配上就返回true, 否则false
	// msg是一个sarama.ConsumerMessage的结构体，消息存储在msg.Value中，它是一个[]byte
	//　所以需要使用reg.Match来匹配，如果是字符串就使用reg.MatchString
	for idx, reg := range regexpList {
		if res := reg.MatchString(m.Message); res {
			return idx, true
		}
	}
	return -1, false
}
