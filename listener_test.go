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
	configs.LogLevel = "disabled"
	s.l = NewListener(configs, appName)
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

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*100)

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

func (s *ListenerTestSuite) TestListener_eventsWorker() {
	eventTypNoErr := "testingListenerApp.test"
	callsEventTypNoErr := 0

	eventTypErr := "testingListenerApp.error"
	callsEventTypErr := 0

	s.l.AddEventHandler(
		eventTypNoErr,
		func(ctx context.Context, e *Event) error {
			callsEventTypNoErr++
			return nil
		},
	)

	s.l.AddEventHandler(
		eventTypErr,
		func(ctx context.Context, e *Event) error {
			callsEventTypErr++
			return errors.New("error")
		},
	)

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*100)

	cmdsCh := make(chan amqp091.Delivery)

	go s.l.eventsWorker(ctx, cmdsCh)

	body, _ := json.Marshal(eventBody{
		Name: eventTypNoErr,
		Data: "data",
	})
	d := amqp091.Delivery{
		Body: body,
	}

	// Send no err
	cmdsCh <- d

	body, _ = json.Marshal(eventBody{
		Name: eventTypErr,
		Data: "data",
	})
	d = amqp091.Delivery{
		Body: body,
	}

	// Send err
	cmdsCh <- d
	cmdsCh <- d

	<-ctx.Done()

	s.Equal(1, callsEventTypNoErr)

	s.Equal(2, callsEventTypErr)
}

func (s *ListenerTestSuite) TestListener_queriesWorker() {
	queriesTypNoErr := "testingListenerApp.test"
	callsQueriesTypNoErr := 0
	data := map[string]interface{}{
		"data": "data",
	}

	queriesTypErr := "testingListenerApp.error"
	callsQueriesTypErr := 0

	s.l.AddQueryHandler(
		queriesTypNoErr,
		func(ctx context.Context, q *Query) (interface{}, error) {
			callsQueriesTypNoErr++
			s.Equal(data, q.Data)

			return nil, nil
		},
	)

	s.l.AddQueryHandler(
		queriesTypErr,
		func(ctx context.Context, q *Query) (interface{}, error) {
			callsQueriesTypErr++
			return nil, errors.New("error")
		},
	)

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*100)

	queriesCh := make(chan amqp091.Delivery)

	go s.l.queriesWorker(ctx, queriesCh)

	body, _ := json.Marshal(queryBody{
		Resource: queriesTypNoErr,
		Data:     data,
	})
	d := amqp091.Delivery{
		CorrelationId: "1",
		ConsumerTag:   "p",
		Body:          body,
	}

	// Send no err
	queriesCh <- d

	body, _ = json.Marshal(queryBody{
		Resource: queriesTypErr,
		Data:     data,
	})
	d = amqp091.Delivery{
		CorrelationId: "1",
		ConsumerTag:   "p",
		Body:          body,
	}

	// Send err
	queriesCh <- d
	queriesCh <- d

	<-ctx.Done()

	s.Equal(1, callsQueriesTypNoErr)

	s.Equal(2, callsQueriesTypErr)
}

func (s *ListenerTestSuite) TestListener_rejectMsg() {
	toElapse := time.Millisecond * 100
	s.l.configs.DelayOnReject = toElapse
	n := time.Now()

	s.l.rejectMsg(&amqp091.Delivery{}, false)

	s.WithinDuration(n.Add(toElapse), time.Now(), time.Millisecond*10)
}
