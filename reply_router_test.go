package rcgo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

func TestReplyRouterTestSuite(t *testing.T) {
	suite.Run(t, new(ReplyRouterTestSuite))
}

type ReplyRouterTestSuite struct {
	suite.Suite
	*replyRouter
}

func (s *ReplyRouterTestSuite) SetupSuite() {
	s.replyRouter = newReplyRouter("test", time.Second*15, 15)
}

func (s *ReplyRouterTestSuite) Test_addReplyToListen() {
	query := "query"
	corrId := "123"
	timeout := time.Millisecond * 100

	s.timeout = timeout

	start := time.Now()

	replyCh := s.replyRouter.addReplyToListen(query, corrId)

	v, ok := s.replyRouter.repliesMap.Load(corrId)
	replyStr := v.(replyStr)
	s.True(ok)
	s.Equal(query, replyStr.query)

	reply := <-replyCh

	s.WithinDuration(start.Add(timeout), time.Now(), time.Millisecond*10)
	s.EqualError(reply.Err, "timeout while waiting for a reply")
}

func (s *ReplyRouterTestSuite) Test_stop() {
	query := "query"
	corrId := "123"
	// Add a significant timeout to check and stop queries waiting for a reply.
	timeout := time.Second * 100
	s.timeout = timeout

	start := time.Now()

	replyCh := s.replyRouter.addReplyToListen(query, corrId)

	v, ok := s.replyRouter.repliesMap.Load(corrId)
	replyStr := v.(replyStr)
	s.True(ok)
	s.Equal(query, replyStr.query)

	s.replyRouter.stop(context.Background())
	reply := <-replyCh

	s.WithinDuration(start, time.Now(), time.Millisecond*200)
	s.EqualError(reply.Err, "context canceled while waiting for a reply")
}
