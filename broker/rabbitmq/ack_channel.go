package rabbitmq

import (
	"sync"

	proto "github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"

	requestQueueProto "bitbucket.com/peti2001/candidate_parser/request/proto/requestQueue"
)

var AckChannel channel

type channel struct {
	//ch map[string]chan amqp.Delivery
	ch map[string]amqp.Delivery
	sync.Mutex
}

func NewAckChannel() channel {
	return channel{
		make(map[string]amqp.Delivery),
		sync.Mutex{},
	}
}

func (c *channel) AddDelivery(topic string, d amqp.Delivery) {
	var msg requestQueueProto.AddToQueueRequest
	proto.Unmarshal(d.Body, &msg)

	c.Lock()
	if _, ok := c.ch[msg.Url]; !ok {
		c.ch[msg.Url] = d
	}
	c.Unlock()
}

//TODO handle timeout, if the handler cannot ack in time
func (c *channel) AckDelivery(topic, url string) {
	c.Lock()
	if d, ok := c.ch[url]; ok {
		d.Ack(false)
		delete(c.ch, url)
	}
	c.Unlock()
}