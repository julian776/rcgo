package rcgo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

// This test suite contains all tests that do not require a running server.
type PublisherTestSuite struct {
	appName string
	suite.Suite
	p *Publisher
}

func (s *PublisherTestSuite) SetupSuite() {
	url := "amqp://user:password@localhost"
	s.appName = "testingPublisherApp"

	configs := NewPublisherDefaultConfigs(url)
	s.p = NewPublisher(configs, s.appName)
}

func TestPublisherTestSuite(t *testing.T) {
	suite.Run(t, new(PublisherTestSuite))
}

func (s *PublisherTestSuite) TestPublisher_New() {
	c := NewPublisherDefaultConfigs("")

	s.Panics(func() {
		NewPublisher(c, s.appName)
	})
}

func (s *PublisherTestSuite) TestPublisher_BlockSendMsgsIfStopped() {
	c := NewPublisherDefaultConfigs("url")

	p := NewPublisher(c, s.appName)
	p.Stop()

	ctx := context.Background()

	err := p.SendCmd(ctx, "tL", "cmd", []byte(""))
	s.ErrorIs(err, ErrPublisherStopped)

	err = p.PublishEvent(ctx, "event", []byte(""))
	s.ErrorIs(err, ErrPublisherStopped)

	res, err := p.RequestReply(ctx, "tL", "cmd", []byte(""))
	s.ErrorIs(err, ErrPublisherStopped)
	s.Equal([]byte{}, res)

	ch, err := p.RequestReplyC(ctx, "tL", "cmd", []byte(""))
	s.Nil(ch)
	s.ErrorIs(err, ErrPublisherStopped)
}

func (s *PublisherTestSuite) TestPublisher_MultipleStops() {
	c := NewPublisherDefaultConfigs("url")

	p := NewPublisher(c, s.appName)

	s.Nil(p.Stop())
	s.Nil(p.Stop())
	s.Nil(p.Stop())
}
