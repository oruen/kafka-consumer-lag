package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/ugorji/go/codec"
	"os"
	"strings"
	"time"
)

var groupIdInput = flag.String("group-id", "", "Comma-separated consumer group ids")

var brokersInput = flag.String("brokers", "", "List of broker addresses with ports, comma separated")

var topicInput = flag.String("topic", "", "Topic name")

var commandInput = flag.String("command", "lag", "Command")

var msgpack codec.MsgpackHandle

type ConsumerOffset struct {
	Topic     string
	Offset    int64
	Partition int32
}

var brokers []string
var kafkaClient sarama.Client

func main() {
	flag.Parse()
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		processWithStdin()
	} else {
		processWithArgs()
	}
}

func processWithStdin() {
	setupClient()
	scanner := bufio.NewScanner(os.Stdin)
	var controlChannel = make(chan int)
	var lines int
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), " ")
		go func() {
			processGroup(*commandInput, parts[0], parts[1])
			controlChannel <- 1
		}()
		lines += 1
	}
	for i := 0; i < lines; i++ {
		_ = <-controlChannel
	}
}

func processWithArgs() {
	checkArgs()
	setupClient()
	processGroup(*commandInput, *groupIdInput, *topicInput)
}

func setupClient() {
	var err error
	if *brokersInput == "" {
		exit(errors.New("Brokers are not defined. Use --brokers argument to define it."))
	}
	brokers = strings.Split(*brokersInput, ",")
	kafkaClient, err = sarama.NewClient(brokers, nil)
	if err != nil {
		exit(err)
	}
}

func checkArgs() {
	if *topicInput == "" {
		exit(errors.New("Topic is not defined. Use --topic argument to define it."))
	}
	if *groupIdInput == "" {
		exit(errors.New("Consumer group is not defined. Use --group-id argument to define it."))
	}
}

func processGroup(command string, groupId string, topic string) {
	partitions, err := kafkaClient.Partitions(topic)
	if err != nil {
		exit(err)
	}
	offsetManager, err := sarama.NewOffsetManagerFromClient(groupId, kafkaClient)
	if err != nil {
		exit(err)
	}
	var lag int64
	var timestamps = make([]int64, len(partitions))
	var controlChannel = make(chan []int64)
	for _, partition := range partitions {
		go processPartition(command, topic, partition, controlChannel, offsetManager)
	}
	for _, partition := range partitions {
		response := <-controlChannel
		lag += response[0]
		if command == "lag_and_time" {
			timestamps[partition] = response[1]
		}
	}
	if command == "lag_and_time" {
		max := timestamps[0]
		for _, value := range timestamps {
			if value > max {
				max = value
			}
		}
		timelag := time.Now().Unix() - max
		fmt.Println(groupId, topic, lag, timelag)
	} else {
		fmt.Println(groupId, topic, lag)
	}
}

func processPartition(command string, topic string, partition int32, controlChannel chan []int64, offsetManager sarama.OffsetManager) {
	pom, err := offsetManager.ManagePartition(topic, int32(partition))
	if err != nil {
		exit(err)
	}
	consumerOffset, _ := pom.NextOffset()
	offset, err := kafkaClient.GetOffset(topic, int32(partition), sarama.OffsetNewest)
	//fmt.Println(topic, partition, consumerOffset, offset)
	if err != nil {
		exit(err)
	}
	var response = make([]int64, 0)
	var timelag int64
	lag := offset - consumerOffset + 1
	response = append(response, lag)
	if command == "lag_and_time" {
		broker, err := kafkaClient.Leader(topic, partition)
		if err != nil {
			exit(err)
		}
		fetchRequest := sarama.FetchRequest{MaxWaitTime: 10000, MinBytes: 0}
		fetchRequest.AddBlock(topic, partition, consumerOffset-2, 100000)
		fetchResponse, err := broker.Fetch(&fetchRequest)
		if err != nil {
			exit(err)
		}
		block := fetchResponse.GetBlock(topic, partition)
		messages := block.MsgSet.Messages
		if len(messages) > 0 {
			msg := messages[0].Messages()[0].Msg.Value
			var decodedData map[string]interface{}
			codec.NewDecoderBytes(msg, &msgpack).Decode(&decodedData)
			timestamp := decodedData["timestamp"]
			switch timestamp := timestamp.(type) {
			case uint64:
				timelag = int64(timestamp)
			default:
				fmt.Println(timestamp)
				exit(errors.New("message is missing timestamp"))
			}
		} else {
			timelag = 0
		}
		response = append(response, timelag)
	}
	controlChannel <- response
}

func exit(err error) {
	fmt.Println(err)
	os.Exit(1)
}
