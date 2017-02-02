package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	docopt "github.com/docopt/docopt-go"
	"github.com/jurriaan/kafkatools"
)

var (
	version     = "0.1"
	gitrev      = "unknown"
	versionInfo = `reset_consumer_group %s (git rev %s)`
	usage       = `reset_consumer_group - a tool to reset the consumer group offset for a specific topic

usage:
  wip --topic <topic> --broker <broker,..> [options]

options:
  -h --help                  show this screen.
  -V, --version              show version.
  -t, --topic <topic>        the topic
  -b, --broker <broker,..>   the brokers to connect to
  -o, --offset <offset>      offset to start consuming from: beginning | end | <value> (absolute offset) | -<value> (relative offset) TODO
  -q, --quiet                be quiet
  -p, --partition <n>        consume a single partition
	--start-date <timestamp>   start consuming from the specified timestamp
	--end-date <timestamp>     stop consuming until the specified timestamp
  -c, --count <n>            stop consuming after n messages
  -e, --exit                 stop consuming after the last message
`
)

func parseDateOpt(dateOpt interface{}) int64 {
	date, err := time.Parse(time.RFC3339, dateOpt.(string))
	if err != nil {
		log.Fatal("Invalid time format specified (RFC3339 required): ", err)
	}

	// Compute time in milliseconds
	return date.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

func main() {
	var endOffsets map[int32]kafkatools.TopicPartitionOffset

	docOpts, err := docopt.Parse(usage, nil, true, fmt.Sprintf(versionInfo, version, gitrev), false)

	if err != nil {
		log.Panicf("[PANIC] We couldn't parse doc opts params: %v", err)
	}

	brokers := strings.Split(docOpts["--broker"].(string), ",")
	topic := docOpts["--topic"].(string)

	client := kafkatools.GetSaramaClient(brokers...)

	offset := sarama.OffsetNewest
	if docOpts["--start-date"] != nil {
		offset = parseDateOpt(docOpts["--start-date"])
	}

	log.Println("Fetching offsets")
	partitionOffsets := kafkatools.FetchTopicOffsets(client, offset, topic)

	if docOpts["--partition"] != nil {
		partition, err := strconv.Atoi(docOpts["--partition"].(string))
		if err != nil {
			log.Fatal("Invalid partition specified: ", err)
		}

		val, found := partitionOffsets[int32(partition)]
		if !found {
			log.Fatalf("Partition %d not found for topic %s", partition, topic)
		}

		partitionOffsets = make(map[int32]kafkatools.TopicPartitionOffset)
		partitionOffsets[val.Partition] = val
	}

	if docOpts["--end-date"] != nil {
		endOffsets = kafkatools.FetchTopicOffsets(client, parseDateOpt(docOpts["--end-date"]), topic)
	} else if docOpts["--exit"].(bool) {
		endOffsets = kafkatools.FetchTopicOffsets(client, sarama.OffsetNewest, topic)
	}

	count := -1
	if docOpts["--count"] != nil {
		count, err = strconv.Atoi(docOpts["--count"].(string))
		if err != nil {
			log.Fatal("Invalid count specified: ", err)
		}
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalf("Could not start consumer: %v", err)
	}

	messages, closing := consumePartitions(consumer, partitionOffsets, endOffsets)

	printMessages(messages, count, func(str string) { fmt.Println(str) })
	close(closing)

	if err = client.Close(); err != nil {
		log.Fatal("Could not properly close the client")
	}

	log.Println("Connection closed. Bye.")
}

func printMessages(messages chan *sarama.ConsumerMessage, maxMessages int, printer func(string)) {
	counter := 0

	for msg := range messages {
		printer(string(msg.Value))

		if maxMessages != -1 {
			counter++
			if counter >= maxMessages {
				log.Printf("Quiting after %d messages", counter)
				return
			}
		}
	}
}

func consumePartitions(consumer sarama.Consumer, partitionOffsets map[int32]kafkatools.TopicPartitionOffset, endOffsets map[int32]kafkatools.TopicPartitionOffset) (messages chan *sarama.ConsumerMessage, closing chan struct{}) {
	var wg sync.WaitGroup
	messages = make(chan *sarama.ConsumerMessage)
	closing = make(chan struct{})

	for _, offset := range partitionOffsets {
		log.Printf("Consuming %s partition %d starting at %d (until %d)", offset.Topic, offset.Partition, offset.Offset, endOffsets[offset.Partition].Offset)
		pc, err := consumer.ConsumePartition(offset.Topic, offset.Partition, offset.Offset)
		if err != nil {
			log.Panicf("ERROR: Failed to start consumer for partition %d: %s", offset.Partition, err)
		}

		partitionCloser := make(chan struct{})
		go func(pc sarama.PartitionConsumer) {
			select {
			case <-closing:
			case <-partitionCloser:
			}
			err := pc.Close()
			if err != nil {
				log.Panicf("ERROR: Failed to close consumer for partition %d: %s", offset.Partition, err)
			}
		}(pc)

		wg.Add(1)
		partitionEndOffset := int64(-1)
		if len(endOffsets) != 0 {
			partitionEndOffset = endOffsets[offset.Partition].Offset
		}

		go func(pc sarama.PartitionConsumer, partitionEndOffset int64, partitionCloser chan struct{}) {
			defer wg.Done()
			for message := range pc.Messages() {
				if partitionEndOffset != -1 {
					if message.Offset >= partitionEndOffset {
						close(partitionCloser)
						break
					}
				}
				messages <- message
			}
		}(pc, partitionEndOffset, partitionCloser)

		go func(pc sarama.PartitionConsumer) {
			for err := range pc.Errors() {
				log.Printf("error: we got an error on one of the partitions: %v", err)
			}
		}(pc)
	}

	go func() {
		wg.Wait()
		if err := consumer.Close(); err != nil {
			log.Fatal("Error closing the consumer: ", err)
		}
		close(messages)
	}()

	return messages, closing
}
