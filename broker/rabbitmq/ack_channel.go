package rabbitmq

import "github.com/streadway/amqp"

var AckChannel channel

type channel struct {
	ch map[string]chan amqp.Delivery
}

func NewAckChannel() channel {
	return channel{
		ch: make(map[string]chan amqp.Delivery),
	}
}

func (c *channel) AddDelivery(topic string, d amqp.Delivery) {
	if _, ok := c.ch[topic]; !ok {
		c.ch[topic] = make(chan amqp.Delivery)
	}
	c.ch[topic] <- d
}

//TODO handle timeout, if the handler cannot ack in time
func (c *channel) AckDelivery(topic string) {
	(<-c.ch[topic]).Ack(false)
}
