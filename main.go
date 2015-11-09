package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"strings"
)

var groupIdInput = flag.String("group-id", "", "Comma-separated consumer group ids")

var brokersInput = flag.String("brokers", "", "List of broker addresses with ports, comma separated")

var topicInput = flag.String("topic", "", "Topic name")

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
			processGroup(parts[1], parts[0])
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
	processGroup(*groupIdInput, *topicInput)
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

func processGroup(groupId string, topic string) {
	partitions, err := kafkaClient.Partitions(topic)
	if err != nil {
		exit(err)
	}
	offsetManager, err := sarama.NewOffsetManagerFromClient(groupId, kafkaClient)
	if err != nil {
		exit(err)
	}
	var lag int64
	var controlChannel = make(chan int64)
	for _, partition := range partitions {
		go processPartition(topic, partition, controlChannel, offsetManager)
	}
	for _ = range partitions {
		partitionLag := <-controlChannel
		lag += partitionLag
	}
	fmt.Println(topic, groupId, lag)
}

func processPartition(topic string, partition int32, controlChannel chan int64, offsetManager sarama.OffsetManager) {
	pom, err := offsetManager.ManagePartition(topic, int32(partition))
	if err != nil {
		exit(err)
	}
	consumerOffset, _ := pom.NextOffset()
	offset, err := kafkaClient.GetOffset(topic, int32(partition), sarama.OffsetNewest)
	if err != nil {
		exit(err)
	}
	controlChannel <- offset - consumerOffset + 1
}

func exit(err error) {
	fmt.Println(err)
	os.Exit(1)
}
