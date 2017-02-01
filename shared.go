package kafkatools

import (
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

// GetSaramaClient sets up a kafka client
func GetSaramaClient(brokers ...string) sarama.Client {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0
	config.Consumer.Return.Errors = true
	config.Metadata.RefreshFrequency = 1 * time.Minute
	config.Metadata.Retry.Max = 10
	config.Net.MaxOpenRequests = 10

	client, err := sarama.NewClient(brokers, config)

	if err != nil {
		log.Fatal("Failed to start client: ", err)
	}

	return client
}

// GetSaramaConsumer returns a high-level kafka consumer
func GetSaramaConsumer(brokers string, consumerGroup string, topics []string) *cluster.Consumer {
	// Init config
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Version = sarama.V0_10_1_0

	consumer, err := cluster.NewConsumer(strings.Split(brokers, ","), consumerGroup, topics, config)
	if err != nil {
		log.Fatalf("Failed to start consumer: %s", err)
	}

	return consumer
}

// GenerateOffsetRequests generates the offset requests which can be used in the GetBrokerTopicOffsets function
func GenerateOffsetRequests(client sarama.Client, time int64, topics ...string) (requests map[*sarama.Broker]*sarama.OffsetRequest) {
	var err error
	requests = make(map[*sarama.Broker]*sarama.OffsetRequest)

	if len(topics) == 0 {
		topics, err = client.Topics()
		if err != nil {
			log.Fatal("Failed to fetch topics: ", err)
		}
	}

	for _, topic := range topics {
		partitions, err := client.Partitions(topic)
		if err != nil {
			log.Fatal("Failed to fetch partitions: ", err)
		}
		for _, partition := range partitions {
			broker, err := client.Leader(topic, partition)
			if err != nil {
				log.Fatalf("Cannot fetch leader for partition %d of topic %s", partition, topic)
			}

			if _, ok := requests[broker]; !ok {
				requests[broker] = &sarama.OffsetRequest{Version: 1}
			}

			requests[broker].AddBlock(topic, partition, time, 1)
		}
	}

	return requests
}

// GetBrokerTopicOffsets fetches the offsets for all topics from a specific groker and sends them to the offset topic
func GetBrokerTopicOffsets(broker *sarama.Broker, request *sarama.OffsetRequest, offsets chan TopicPartitionOffset) {
	response, err := broker.GetAvailableOffsets(request)
	if err != nil {
		log.Fatalf("Cannot fetch offsets from broker %d: %v", broker.ID(), err)
	}
	for topic, partitions := range response.Blocks {
		for partition, offsetResponse := range partitions {
			if offsetResponse.Err != sarama.ErrNoError {
				log.Printf("Error in OffsetResponse for topic %s:%d from broker %d: %s", topic, partition, broker.ID(), offsetResponse.Err.Error())
				continue
			}
			if len(offsetResponse.Offsets) == 1 {
				offsets <- TopicPartitionOffset{
					Partition: partition,
					Offset:    offsetResponse.Offset,
					Topic:     topic,
				}
			}
		}
	}
}

// FetchTopicOffsets fetches topic offsets
func FetchTopicOffsets(client sarama.Client, offset int64, topic string) (topicOffsets map[int32]TopicPartitionOffset) {
	requests := GenerateOffsetRequests(client, offset, topic)

	var wg, wg2 sync.WaitGroup
	topicOffsetChannel := make(chan TopicPartitionOffset, 20)
	wg.Add(len(requests))
	for broker, request := range requests {
		// Fetch topic offsets (log end)
		go func(broker *sarama.Broker, request *sarama.OffsetRequest) {
			defer wg.Done()
			GetBrokerTopicOffsets(broker, request, topicOffsetChannel)
		}(broker, request)
	}

	// Setup lookup table for topic offsets
	topicOffsets = make(map[int32]TopicPartitionOffset)
	go func() {
		defer wg2.Done()
		wg2.Add(1)
		for topicOffset := range topicOffsetChannel {
			topicOffsets[topicOffset.Partition] = topicOffset
		}
	}()

	wg.Wait()
	close(topicOffsetChannel)
	wg2.Wait()

	return topicOffsets
}

// FetchOffsets fetches group and topic offsets (where the topic offset can be sarama.OffsetNewest/OffsetOldest or the time in milliseconds)
func FetchOffsets(client sarama.Client, offset int64) (groupOffsets GroupOffsetSlice, topicOffsets map[string]map[int32]TopicPartitionOffset) {
	requests := GenerateOffsetRequests(client, offset)

	var wg, wg2 sync.WaitGroup
	topicOffsetChannel := make(chan TopicPartitionOffset, 20)
	groupOffsetChannel := make(chan GroupOffset, 10)

	wg.Add(2 * len(requests))
	for broker, request := range requests {
		// Fetch topic offsets (log end)
		go func(broker *sarama.Broker, request *sarama.OffsetRequest) {
			defer wg.Done()
			GetBrokerTopicOffsets(broker, request, topicOffsetChannel)
		}(broker, request)

		// Fetch group offsets
		go func(broker *sarama.Broker) {
			defer wg.Done()
			GetBrokerGroupOffsets(broker, groupOffsetChannel)
		}(broker)
	}

	// Setup lookup table for topic offsets
	topicOffsets = make(map[string]map[int32]TopicPartitionOffset)
	go func() {
		defer wg2.Done()
		wg2.Add(1)
		for topicOffset := range topicOffsetChannel {
			if _, ok := topicOffsets[topicOffset.Topic]; !ok {
				topicOffsets[topicOffset.Topic] = make(map[int32]TopicPartitionOffset)
			}
			topicOffsets[topicOffset.Topic][topicOffset.Partition] = topicOffset
		}
	}()

	go func() {
		defer wg2.Done()
		wg2.Add(1)
		for offset := range groupOffsetChannel {
			groupOffsets = append(groupOffsets, offset)
		}
		sort.Sort(groupOffsets)
	}()

	// wait for goroutines to finish
	wg.Wait()
	close(topicOffsetChannel)
	close(groupOffsetChannel)
	wg2.Wait()

	return
}

// GetBrokerGroupOffsets fetches all group offsets for a specific broker
func GetBrokerGroupOffsets(broker *sarama.Broker, groupOffsetChannel chan GroupOffset) {
	groupsResponse, err := broker.ListGroups(&sarama.ListGroupsRequest{})
	if err != nil {
		log.Fatal("Failed to list groups: ", err)
	}
	var groups []string
	for group := range groupsResponse.Groups {
		groups = append(groups, group)
	}
	groupsDesc, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groups})
	if err != nil {
		log.Fatal("Failed to describe groups: ", err)
	}

	var wg sync.WaitGroup
	wg.Add(len(groupsDesc.Groups))

	for _, desc := range groupsDesc.Groups {
		go func(desc *sarama.GroupDescription) {
			defer wg.Done()
			var offset GroupOffset
			offset.Group = desc.GroupId

			request := GetOffsetFetchRequest(desc)

			offsets, err := broker.FetchOffset(request)
			if err != nil {
				log.Fatal("Failed to fetch offsets")
			}

			for topic, partitionmap := range offsets.Blocks {
				groupTopic := GroupTopicOffset{Topic: topic}
				for partition, block := range partitionmap {
					topicPartition := TopicPartitionOffset{Partition: partition, Offset: block.Offset, Topic: topic}
					groupTopic.TopicPartitionOffsets = append(groupTopic.TopicPartitionOffsets, topicPartition)
				}
				sort.Sort(groupTopic.TopicPartitionOffsets)
				offset.GroupTopicOffsets = append(offset.GroupTopicOffsets, groupTopic)
			}

			sort.Sort(offset.GroupTopicOffsets)
			groupOffsetChannel <- offset
		}(desc)
	}
	wg.Wait()
}

// GetOffsetFetchRequest generates a request for the offsets of a specific group
func GetOffsetFetchRequest(desc *sarama.GroupDescription) *sarama.OffsetFetchRequest {
	request := new(sarama.OffsetFetchRequest)
	request.Version = 1
	request.ConsumerGroup = desc.GroupId

	for _, memberDesc := range desc.Members {
		assignArr := memberDesc.MemberAssignment
		if len(assignArr) == 0 {
			continue
		}

		assignment := ParseMemberAssignment(assignArr)
		for _, topicAssignment := range assignment.Assignments {
			for _, partition := range topicAssignment.Partitions {
				request.AddPartition(topicAssignment.Topic, partition)
			}
		}
	}

	return request
}
