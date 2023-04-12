package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

var (
	// go run main.go -addr=localhost:9092
	addr      = flag.String("addr", "localhost:9092", "-addr=localhost:9092")
	topic     = "my-topic"
	partition = 0
	conn      *kafka.Conn
)

func init() {
	flag.Parse()

	connectKafka()
}

// connectKafka 连接到 kafka
func connectKafka() {
	var err error
	conn, err = kafka.DialLeader(context.Background(), "tcp", *addr, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
}

func sendMsgToKafka() {
	_, err := conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	log.Println("send message to kafka successfully")
}

// sendMsgToKafkaWithNum 发送 num 条消息到 kafka
func sendMsgToKafkaWithNum(num int) {
	var messages []kafka.Message
	for i := 0; i < num; i++ {
		messages = append(messages, kafka.Message{Value: []byte(fmt.Sprintf("message-%d", i))})
	}

	res, err := conn.WriteMessages(messages...)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
	log.Printf("send %d message to kafka successfully\n", res)
}

// consumeSingleMsgFromKafka 消费单条消息
func consumeSingleMsgFromKafka() {
	message, err := conn.ReadMessage(1024)
	if err != nil {
		panic(err)
	}
	log.Println(message)
}

// consumeBatchMsgFromKafka 消费批量消息
func consumeBatchMsgFromKafka() {
	// 批量读取，所有消息需要达到这个阈值，才能读取的到
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
		n, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b[:n]))
	}

	if err := batch.Close(); err != nil {
		log.Fatal("failed to close batch:", err)
	}
}

func main() {
	defer func() {
		_ = conn.Close()
	}()

	//sendMsgToKafka()
	//consumeSingleMsgFromKafka()

	sendMsgToKafkaWithNum(1000)
	consumeBatchMsgFromKafka()
}
