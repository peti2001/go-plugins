package rabbitmq_test

import (
	"testing"

	//"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/peti2001/go-plugins/broker/rabbitmq"
)

type mockDelivery struct {
	mock.Mock
}

func (m *mockDelivery) Ack(b bool) error {
	m.Called(b)

	return nil
}

func TestChannel_AckDelivery(t *testing.T) {
	//Arrange
	delivery1 := new(mockDelivery)
	delivery2 := new(mockDelivery)
	delivery3 := new(mockDelivery)
	delivery4 := new(mockDelivery)
	delivery5 := new(mockDelivery)
	delivery6 := new(mockDelivery)
	ackChannel := rabbitmq.NewAckChannel()
	delivery1.On("Ack", false).Return(nil)
	delivery2.On("Ack", false).Return(nil)
	delivery3.On("Ack", false).Return(nil)
	delivery4.On("Ack", false).Return(nil)
	delivery5.On("Ack", false).Return(nil)
	delivery6.On("Ack", false).Return(nil)

	//Act
	//One at the same time
	ackChannel.AddDelivery("http://test.hu", delivery1)
	ackChannel.AckDelivery("http://test.hu")
	delivery1.AssertCalled(t, "Ack", false)

	//Two at the same time
	ackChannel.AddDelivery("http://test.hu", delivery2)
	ackChannel.AddDelivery("http://test.hu", delivery3)
	ackChannel.AckDelivery("http://test.hu")
	delivery2.AssertCalled(t, "Ack", false)
	ackChannel.AckDelivery("http://test.hu")
	delivery3.AssertCalled(t, "Ack", false)

	//Different urls
	ackChannel.AddDelivery("http://test2.hu", delivery4)
	ackChannel.AddDelivery("http://test2.hu", delivery5)
	ackChannel.AddDelivery("http://test.hu", delivery6)
	ackChannel.AckDelivery("http://test2.hu")
	delivery4.AssertCalled(t, "Ack", false)
	ackChannel.AckDelivery("http://test.hu")
	delivery6.AssertCalled(t, "Ack", false)
	ackChannel.AckDelivery("http://test2.hu")
	delivery5.AssertCalled(t, "Ack", false)
}
