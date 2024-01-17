package rcgo

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/suite"
)

const (
	appName = "testingListenerApp"
)

// This test suite contains all tests that do not require a running server.
type ListenerTestSuite struct {
	suite.Suite
	l *Listener
}

func (s *ListenerTestSuite) SetupSuite() {
	url := "amqp://user:password@localhost"

	configs := NewListenerDefaultConfigs(url)
	s.l = NewListener(*configs, appName)
}

func TestListenerTestSuite(t *testing.T) {
	suite.Run(t, new(ListenerTestSuite))
}

func (s *ListenerTestSuite) TestListener_cmdsWorker() {
	cmdTypNoErr := "testingListenerApp.test"
	callsCmdTypNoErr := 0

	cmdTypErr := "testingListenerApp.error"
	callsCmdTypErr := 0

	s.l.AddCommandHandler(
		cmdTypNoErr,
		func(ctx context.Context, c *Cmd) error {
			callsCmdTypNoErr++
			return nil
		},
	)

	s.l.AddCommandHandler(
		cmdTypErr,
		func(ctx context.Context, c *Cmd) error {
			callsCmdTypErr++
			return errors.New("error")
		},
	)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*1)

	cmdsCh := make(chan amqp091.Delivery)

	go s.l.cmdsWorker(ctx, cmdsCh)

	body, _ := json.Marshal(cmdBody{
		Name:  cmdTypNoErr,
		CmdId: "1",
		Data:  "data",
	})
	d := amqp091.Delivery{
		Body: body,
	}

	// Send no err
	cmdsCh <- d

	body, _ = json.Marshal(cmdBody{
		Name:  cmdTypErr,
		CmdId: "1",
		Data:  "data",
	})
	d = amqp091.Delivery{
		Body: body,
	}

	// Send err
	cmdsCh <- d

	<-ctx.Done()

	s.Equal(1, callsCmdTypNoErr)

	s.Equal(1, callsCmdTypErr)
}
