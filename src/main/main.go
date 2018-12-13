package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"encoding/json"
	"LogFilter/src/consumer"
	"LogFilter/src/producer"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/go-redis/redis"
)

//filebeat发往kafka的消息结构，其中不需要的字段都使用空接口来接收，在反序列化的时候会自动的对其进行转换
type KafkaMessage struct {
	Timestamp interface{} `json:"@timestamp"`
	Metadata interface{} `json:"@metadata"`
	Source interface{} `json:"source"`
	Fields interface{} `json:"fields"`
	Host interface{} `json:"host"`
	Offset int `json:"offset"`
	Stream interface{} `json:"stream"`
	Message string `json:"message"`
	Prospector interface{} `json:"prospector"`
	Input interface{} `json:"input"`
	Kubernetes interface{} `json:"kubernetes"`
	Beat interface{} `json:"beat"`
}

var (
	// srcTopics = []string{"aixue-qa-logs", "aixue-uat-logs", "aixue-prod-logs"}
	srcTopics = []string{"aixue-qa-logs"}
	filterRules = []string{
		`^127\.0\.0\.1 - .*?"[A-Z]{3,7} /index.php" \d{3}$`, 
		`(?i:error)`,
	}
	regexpList []*regexp.Regexp
	redisClient *redis.Client
)


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
			log.Printf("Topic = %s Partition = %d Offset = %d msg = %s", msg.Topic, msg.Partition, 
						msg.Offset, string(msg.Value))
			//先确定消息已经消费
			c.MarkOffset(msg, "")
			
			//序列化消息
			// var logEntry *KafkaMessage
			// logEntry = &KafkaMessage{}
			logEntry := &KafkaMessage{}
			err := json.Unmarshal(msg.Value, logEntry)
			if err != nil {
				log.Printf("反序列化消息失败, 消息＝%v\terr=%v\n", string(msg.Value), err)
				continue
			}

			//然后对消息进行过滤
			// 1、丢掉127.0.0.1开头的无用访问日志
			// 2、挑选出带有err, error的日志，并将其发送到redis中；
			if pos, res := Filter(logEntry); res {
				switch pos {
				case 0:
					//匹配 `^127.0.0.1`
					// log.Printf("丢弃消息{ %s }\n", logEntry.Message)
					log.Println("丢弃一条消息")
				case 1:
					//匹配 `(?i:error)`
					// sendMsgToRedis
					// log.Printf("将消息{ %s }发住redis中\n", logEntry.Message)
					redisClient.LPush("console-error", logEntry.Message)
				}
			}else {
				//未匹配，再把该消息发送到dstTopic中
				// log.Printf("消息{ %v }正常通过\n", string(msg.Value))
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
		Topic: fmt.Sprintf("%s-pure", msg.Topic),
		Value: sarama.ByteEncoder(msg.Value), //将msg.Value转换成Encoder类型，这样才能当ProducerMessage的value使用
	}
	// partition, offset, err := p.SendMessage(pmsg)
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


func main(){
	//编译正则表达式，存入正则列表
	for _, v := range filterRules {
		regexpList = append(regexpList, regexp.MustCompile(v))
	}

	//初始化redisClient
	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		Password: "",
		DB: 0,
	})
	defer redisClient.Close()

	//捕获os.Interrupt信号，在消费时如果捕获到，就正常退出
	//这里会根据srcTopics的数量去启动相应数量的Consumer，所以需要每一个conSumer都需要通知。
	signals := make(chan os.Signal, 1)
	exitChan := make(chan int, len(srcTopics))
	signal.Notify(signals, os.Interrupt)
	

	//创建一个任务chan，当consumer退出的时候向该chan中放一个数
	done := make(chan int, len(srcTopics))

	//创建对应的生产者
	producerList := []sarama.SyncProducer{}
	for _, _ = range srcTopics {
		p := producer.NewKafkaProducer()
		producerList = append(producerList, p)
	}
	
	//只有一个topic要消费时，创建单个的生产者
	// p := producer.NewKafkaProducer()
	

	//从srcTopics中消费消息，
	for i, t := range srcTopics {
		c := consumer.NewKafkaConsumer(fmt.Sprintf("%v-0", t), append([]string{}, t))
		go ConsumeTopicMessages(c, producerList[i], exitChan, done, redisClient)
	}

	//只有一个topic要消费时，创建单个消费者
	// c := consumer.NewKafkaConsumer(fmt.Sprintf("%v-0", srcTopics[0]), append([]string{}, srcTopics[0]))
	// go ConsumeTopicMessages(c, p, exitChan, done, redisClient)


	//通过os.Interrupt信号知道要退出程序，然后再通过exitChan来通知Consumer退出
	//由于有多个consumer，所以需要一个一个通知。即不停的向exitChan中发数字
	count := 0
	for {
		select {
		case <-signals:
			// log.Println("ctrl＋c ...")
			exitChan<-1
		case <-done:
			count++
			if count == len(srcTopics) {
				log.Println("所有的Producer和Consumer都已经退出，主程序也退出...")
				return
			}
			exitChan<-1
		}
	}
}