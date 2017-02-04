package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/jurriaan/kafkatools"
)

var messages = []*sarama.ConsumerMessage{
	&sarama.ConsumerMessage{Value: []byte("a"), Offset: 1},
	&sarama.ConsumerMessage{Value: []byte("b"), Offset: 2},
	&sarama.ConsumerMessage{Value: []byte("c"), Offset: 3},
	&sarama.ConsumerMessage{Value: []byte("d"), Offset: 4},
}

func TestParseDateOpt(t *testing.T) {
        output := parseDateOpt("2008-09-08T22:47:31-07:00")

        if output != 1220939251000 {
                t.Errorf("%d does not equal 1220939251000 (ms)", output)
        }
}

func TestPrintMessagesCount(t *testing.T) {
	var strings []string

	channel := make(chan *sarama.ConsumerMessage)
	go func() {
		for _, message := range messages {
			channel <- message
		}
		close(channel)
	}()

	printMessages(channel, 2, func(str string) { strings = append(strings, str) })

	if !reflect.DeepEqual(strings, []string{"a", "b"}) {
		t.Errorf("%+v does not equal [a b]", strings)
	}
}

func TestPrintMessagesWithoutCount(t *testing.T) {
	var strings []string

	channel := make(chan *sarama.ConsumerMessage)
	go func() {
		for _, message := range messages {
			channel <- message
		}
		close(channel)
	}()

	printMessages(channel, -1, func(str string) { strings = append(strings, str) })

	if !reflect.DeepEqual(strings, []string{"a", "b", "c", "d"}) {
		t.Errorf("%+v does not equal [a b c d]", strings)
	}
}

func TestConsumeWithoutPartitions(t *testing.T) {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0
	config.Consumer.Return.Errors = true
	config.Metadata.RefreshFrequency = 1 * time.Minute
	config.Metadata.Retry.Max = 10
	config.Net.MaxOpenRequests = 10

	consumer := mocks.NewConsumer(t, config)

	partitionOffsets := make(map[int32]kafkatools.TopicPartitionOffset)
	endOffsets := make(map[int32]kafkatools.TopicPartitionOffset)

	messagesChan, _ := consumePartitions(consumer, partitionOffsets, endOffsets)

	if _, ok := <-messagesChan; ok {
		t.Error("Channel should be closed automatically")
	}
}

func TestConsumeSinglePartition(t *testing.T) {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0
	config.Consumer.Return.Errors = true
	config.Metadata.RefreshFrequency = 1 * time.Minute
	config.Metadata.Retry.Max = 10
	config.Net.MaxOpenRequests = 10

	consumer := mocks.NewConsumer(t, config)

	partitionOffsets := make(map[int32]kafkatools.TopicPartitionOffset)
	partitionOffsets[0] = kafkatools.TopicPartitionOffset{
		Topic:     "foo",
		Partition: 0,
		Offset:    0,
	}
	endOffsets := make(map[int32]kafkatools.TopicPartitionOffset)

	partConsumer := consumer.ExpectConsumePartition("foo", 0, 0)

	for _, msg := range messages {
		partConsumer.YieldMessage(msg)
	}

	partConsumer.ExpectMessagesDrainedOnClose()

	messagesChan, closing := consumePartitions(consumer, partitionOffsets, endOffsets)

	count := 0
	for msg := range messagesChan {
		if msg != messages[count] {
			t.Error("Unexpected message received")
		}

		count++
		if count == len(messages) {
			close(closing)
		}
	}

	if _, ok := <-messagesChan; ok {
		t.Error("Channel should be closed automatically")
	}
}

func TestConsumeSinglePartitionUntilEndOffset(t *testing.T) {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0
	config.Consumer.Return.Errors = true
	config.Metadata.RefreshFrequency = 1 * time.Minute
	config.Metadata.Retry.Max = 10
	config.Net.MaxOpenRequests = 10

	consumer := mocks.NewConsumer(t, config)

	partitionOffsets := make(map[int32]kafkatools.TopicPartitionOffset)
	partitionOffsets[0] = kafkatools.TopicPartitionOffset{
		Topic:     "foo",
		Partition: 0,
		Offset:    0,
	}
	endOffsets := make(map[int32]kafkatools.TopicPartitionOffset)
	endOffsets[0] = kafkatools.TopicPartitionOffset{
		Topic:     "foo",
		Partition: 0,
		Offset:    3,
	}

	partConsumer := consumer.ExpectConsumePartition("foo", 0, 0)

	for _, msg := range messages {
		partConsumer.YieldMessage(msg)
	}

	messagesChan, _ := consumePartitions(consumer, partitionOffsets, endOffsets)

	count := 0
	for msg := range messagesChan {
		if msg != messages[count] {
			t.Error("Unexpected message received")
		}

		count++
	}

	if count != 2 {
		t.Errorf("Expected to receive two messages, received %d", count)
	}

	if _, ok := <-messagesChan; ok {
		t.Error("Channel should be closed automatically")
	}
}
