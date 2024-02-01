package rcgo

import (
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
