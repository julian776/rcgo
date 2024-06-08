package rcgo

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
)

// This test suite contains all tests that do not require a running server.
type ListenerTestSuite struct {
	appName string
	suite.Suite
	l *Listener
}

func (s *ListenerTestSuite) SetupSuite() {
	url := "amqp://user:password@localhost"
	s.appName = "testingListenerApp"

	configs := NewListenerDefaultConfigs(url)
	configs.DelayOnReject = time.Millisecond
	configs.LogLevel = "disabled"
	s.l = NewListener(configs, s.appName)
}

func TestListenerTestSuite(t *testing.T) {
	suite.Run(t, new(ListenerTestSuite))
}

func (s *ListenerTestSuite) TestListener_New() {
	c := NewListenerDefaultConfigs("")

	s.Panics(func() {
		NewListener(c, s.appName)
	})

	c = NewListenerDefaultConfigs("amqp://user:password@localhost")
	c.CmdsWorkers = 0

	s.Panics(func() {
		NewListener(c, s.appName)
	})
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
		Data: map[string]interface{}{
			"data": "data",
		},
	})
	d := amqp091.Delivery{
		Body: body,
	}

	// Send no err
	cmdsCh <- d

	body, _ = json.Marshal(cmdBody{
		Name:  cmdTypErr,
		CmdId: "1",
		Data: map[string]interface{}{
			"data": "data",
		},
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
		Data: map[string]interface{}{
			"data": "data",
		},
	})
	d := amqp091.Delivery{
		Body: body,
	}

	// Send no err
	cmdsCh <- d

	body, _ = json.Marshal(eventBody{
		Name: eventTypErr,
		Data: map[string]interface{}{
			"data": "data",
		},
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

func (s *ListenerTestSuite) TestListener_MultipleStops() {
	s.Nil(s.l.Stop())
	s.Nil(s.l.Stop())
	s.Nil(s.l.Stop())
}

func (s *ListenerTestSuite) TestListener_processQueryErrLogs() {
	b := &strings.Builder{}
	log.Logger = zerolog.New(b).With().Logger()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	body, err := json.Marshal(queryBody{
		Resource: "test",
		Data:     map[string]interface{}{},
	})
	if err != nil {
		s.T().Fatal(err)
	}

	type args struct {
		ctx context.Context
		msg *amqp.Delivery
	}
	tests := []struct {
		name string
		args args
		log  string
	}{
		{
			name: "NoBody",
			args: args{
				ctx: context.Background(),
				msg: &amqp.Delivery{},
			},
			log: "can not process msg",
		},
		{
			name: "NoCorrelationId",
			args: args{
				ctx: context.Background(),
				msg: &amqp.Delivery{
					Body: body,
				},
			},
			log: "correlationID not found. Can not reply",
		},
		{
			name: "NoHandlers",
			args: args{
				ctx: context.Background(),
				msg: &amqp.Delivery{
					Body:          body,
					CorrelationId: "1",
				},
			},
			log: "ignoring msg due to no handler registered, msg type [typeEventNoHandlers]",
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			s.l.processQuery(tt.args.ctx, tt.args.msg)

			str := b.String()
			if !strings.Contains(str, tt.log) {
				t.Errorf("Expected log to contain %s, got %s", tt.log, str)
			}

			b.Reset()
		})
	}
}

func (s *ListenerTestSuite) TestListener_processEventErrLogs() {
	b := &strings.Builder{}
	log.Logger = zerolog.New(b).With().Logger()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	body, err := json.Marshal(eventBody{
		Name: "typeEventNoHandlers",
		Data: map[string]interface{}{},
	})
	if err != nil {
		s.T().Fatal(err)
	}

	type args struct {
		ctx context.Context
		msg *amqp.Delivery
	}
	tests := []struct {
		name string
		args args
		log  string
	}{
		{
			name: "NoBody",
			args: args{
				ctx: context.Background(),
				msg: &amqp.Delivery{},
			},
			log: "can not process msg",
		},
		{
			name: "NoHandlers",
			args: args{
				ctx: context.Background(),
				msg: &amqp.Delivery{
					Body: body,
				},
			},
			log: "ignoring msg due to no handler registered, msg type [typeEventNoHandlers]",
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			s.l.processEvent(tt.args.ctx, tt.args.msg)

			str := b.String()
			if !strings.Contains(str, tt.log) {
				t.Errorf("Expected log to contain %s, got %s", tt.log, str)
			}

			b.Reset()
		})
	}
}

func (s *ListenerTestSuite) TestListener_processCmdErrLogs() {
	b := &strings.Builder{}
	log.Logger = zerolog.New(b).With().Logger()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	body, err := json.Marshal(cmdBody{
		Name: "typeCmdNoHandlers",
		Data: map[string]interface{}{},
	})
	if err != nil {
		s.T().Fatal(err)
	}

	type args struct {
		ctx context.Context
		msg *amqp.Delivery
	}
	tests := []struct {
		name string
		args args
		log  string
	}{
		{
			name: "NoBody",
			args: args{
				ctx: context.Background(),
				msg: &amqp.Delivery{},
			},
			log: "can not process msg",
		},
		{
			name: "NoHandlers",
			args: args{
				ctx: context.Background(),
				msg: &amqp.Delivery{
					Body: body,
				},
			},
			log: "ignoring msg due to no handler registered, msg type [typeCmdNoHandlers]",
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			s.l.processCmd(tt.args.ctx, tt.args.msg)

			str := b.String()
			if !strings.Contains(str, tt.log) {
				t.Errorf("Expected log to contain %s, got %s", tt.log, str)
			}

			b.Reset()
		})
	}
}
