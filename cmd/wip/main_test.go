package main

import (
	"reflect"
	"testing"

	"github.com/Shopify/sarama"
)

var messages = []*sarama.ConsumerMessage{
	&sarama.ConsumerMessage{Value: []byte("a")},
	&sarama.ConsumerMessage{Value: []byte("b")},
	&sarama.ConsumerMessage{Value: []byte("c")},
	&sarama.ConsumerMessage{Value: []byte("d")},
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
