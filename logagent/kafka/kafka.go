package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

var (
	client  sarama.SyncProducer
	msgChan chan *sarama.ProducerMessage
)

func Init(address []string, chanSize int64) (err error) {
	// 1. 生产者配置
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	// 2. 连接kafka
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("kafka:producer closed, err:", err)
		return
	}

	// 初始化MsgChan
	msgChan = make(chan *sarama.ProducerMessage, chanSize)
	// 起一个后台的goroutine从msgchan中读数据
	go sendMsg()
	return
}

func sendMsg() {
	for {
		select {
		case msg := <-msgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Error("send msg failed, err:", err)
				return
			}
			logrus.Infof("pid:%v offset:%v\n", pid, offset)
		}
	}
}

func ToMsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg
}
