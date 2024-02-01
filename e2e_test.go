package rcgo

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// This test suite contains all tests that require a running server.
type E2ETestSuite struct {
	suite.Suite
	ctx context.Context
	url string

	lApp string
	l    *Listener

	pApp string
	p    *Publisher
}

func (s *E2ETestSuite) SetupSuite() {
	s.url = "amqp://user:password@localhost"
	s.lApp = "testingListenerApp"
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	s.ctx = ctx

	lconfigs := NewListenerDefaultConfigs(s.url)
	lconfigs.LogLevel = "disabled"
	s.l = NewListener(lconfigs, s.lApp)

	pconfigs := NewPublisherDefaultConfigs(s.url)
	s.p = NewPublisher(pconfigs, s.pApp)
	s.p.Start(s.ctx)
}

func TestE2ETestSuite(t *testing.T) {
	suite.Run(t, new(E2ETestSuite))
}

func (s *E2ETestSuite) TestE2E_Cmds() {
	cmdTyp := fmt.Sprintf("%s.%s", s.lApp, "cmd")
	var data interface{} = "data"
	wg := sync.WaitGroup{}
	wg.Add(1)

	s.l.AddCommandHandler(
		cmdTyp,
		func(ctx context.Context, c *Cmd) error {
			s.Len(c.Id, 36)
			s.Equal(s.pApp, c.Source)
			s.Equal(s.lApp, c.Target)
			s.Equal(cmdTyp, c.Type)
			s.WithinDuration(time.Now(), c.GenerationTime, time.Second*1)
			s.Exactly(data, c.Data)

			wg.Done()
			return nil
		})

	go func() {
		err := s.l.Listen(s.ctx)
		s.Nil(err)
	}()

	s.p.SendCmd(s.ctx, s.lApp, cmdTyp, data)

	wg.Wait()
}

func (s *E2ETestSuite) TestE2E_Events() {
	eventTyp := "orderPlaced"
	var data interface{} = "data"

	// These ids are used to ensure that both
	// handlers receive the same id.
	var id1 string
	var id2 string

	wg := sync.WaitGroup{}
	wg.Add(2)

	s.l.AddEventHandler(
		eventTyp,
		func(ctx context.Context, e *Event) error {
			s.Len(e.Id, 36)
			id1 = e.Id
			s.Equal(s.pApp, e.Source)
			s.Equal(eventTyp, e.Type)
			s.WithinDuration(time.Now(), e.GenerationTime, time.Second*1)
			s.Exactly(data, e.Data)

			wg.Done()
			return nil
		})

	go func() {
		err := s.l.Listen(s.ctx)
		s.Nil(err)
	}()

	// In this test, we generate an additional listener
	// to verify the broadcast functionality of events
	// to all apps that register for any event.
	configsOtherListener := NewListenerDefaultConfigs(s.url)
	configsOtherListener.LogLevel = "disabled"
	otherListener := NewListener(configsOtherListener, "otherListener")

	otherListener.AddEventHandler(
		eventTyp,
		func(ctx context.Context, e *Event) error {
			s.Len(e.Id, 36)
			id2 = e.Id
			s.Equal(s.pApp, e.Source)
			s.Equal(eventTyp, e.Type)
			s.WithinDuration(time.Now(), e.GenerationTime, time.Second*1)
			s.Exactly(data, e.Data)

			wg.Done()
			return nil
		})

	go func() {
		err := otherListener.Listen(s.ctx)
		s.Nil(err)
	}()

	s.p.PublishEvent(s.ctx, eventTyp, data)

	wg.Wait()

	s.Exactly(id1, id2)
}

func (s *E2ETestSuite) TestE2E_Queries() {
	queryTyp := fmt.Sprintf("%s.%s", s.lApp, "query")
	var data interface{} = "data"
	var dataRes interface{} = "dataRes"
	wg := sync.WaitGroup{}
	wg.Add(1)

	s.l.AddQueryHandler(
		queryTyp,
		func(ctx context.Context, q *Query) (interface{}, error) {
			s.Equal(queryTyp, q.Target)
			s.Equal(queryTyp, q.Type)
			s.WithinDuration(time.Now(), q.GenerationTime, time.Second*1)
			s.Exactly(data, q.Data)

			wg.Done()
			return dataRes, nil
		})

	go func() {
		err := s.l.Listen(s.ctx)
		s.Nil(err)
	}()

	var res interface{}
	s.p.RequestReply(s.ctx, s.lApp, queryTyp, data, &res)

	wg.Wait()

	s.Exactly(dataRes, res)
}

func (s *E2ETestSuite) TestE2E_Close() {
	go func() {
		err := s.l.Listen(s.ctx)
		s.Nil(err)
	}()

	time.Sleep(time.Millisecond * 100)

	err := s.l.Stop()
	s.Nil(err)
}
