package rabbitmq

import (
	"sync"
	"log"

	proto "github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"

	requestQueueProto "bitbucket.com/peti2001/candidate_parser/request/proto/requestQueue"
)

var AckChannel channel

type ackAble interface {
	Ack(bool) error
}

type channel struct {
	ch map[string][]ackAble
	sync.Mutex
}

func NewAckChannel() channel {
	return channel{
		make(map[string][]ackAble),
		sync.Mutex{},
	}
}

func (c *channel) AddDeliveryByAmqp(d amqp.Delivery) {
	var msg requestQueueProto.AddToQueueRequest
	proto.Unmarshal(d.Body, &msg)

	c.AddDelivery(msg.Url, d)
}

func (c *channel) AddDelivery(url string, d ackAble) {
	c.Lock()
	if m, ok := c.ch[url]; !ok {
		m = make([]ackAble, 1)
		m[0] = d
		c.ch[url] = m
	} else {
		m = append(m, d)
		c.ch[url] = m
	}
	c.Unlock()
}

//TODO handle timeout, if the handler cannot ack in time
func (c *channel) AckDelivery(url string) {
	c.Lock()
	if d, ok := c.ch[url]; ok {
		d[0].Ack(false)
		if len(d) == 1 {
			delete(c.ch, url)
		} else {
			d = d[1:]
			c.ch[url] = d
		}
	} else {
		log.Println("Message is missing: " + url)
	}
	c.Unlock()
}
