package rcgo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

type Reply struct {
	Query string
	Data  []byte
	Err   error
}

type replyStr struct {
	query string
	ch    chan *Reply

	// Timer to delete the reply when timeout.
	timer *time.Timer
}

type replyRouter struct {
	id            string
	ch            *amqp.Channel
	repliesMap    sync.Map
	timeout       time.Duration
	prefetchCount int
}

func newReplyRouter(
	appName string,
	timeout time.Duration,
	prefetchCount int,
) *replyRouter {
	if timeout < time.Millisecond*50 {
		log.Panic().Msg("Your timeout is too short, please consider give enough timeout to your replies.")
	}

	return &replyRouter{
		id:            fmt.Sprintf("%s.%s", appName, uuid.NewString()),
		repliesMap:    sync.Map{},
		timeout:       timeout,
		prefetchCount: prefetchCount,
	}
}

// stop will cancel all replies, sending a reply
// with the [rcgo.ErrCanceledReply] error, and
// subsequently closing the channels for each of
// them using the provided context.
func (r *replyRouter) stop(ctx context.Context) error {
	// Wait for a moment to ensure that all replies
	// that can be added while the publisher is stopped are received.
	time.Sleep(time.Millisecond * 100)

	r.repliesMap.Range(func(_ interface{}, v interface{}) bool {
		replyStr := v.(replyStr)
		select {
		case <-ctx.Done():
			ctx.Err()
			return false
		default:
		}

		if !replyStr.timer.Stop() {
			<-replyStr.timer.C
			return true
		}

		replyStr.ch <- &Reply{
			Query: replyStr.query,
			Err:   ErrCanceledReply,
		}

		close(replyStr.ch)
		return true
	})

	return nil
}

func (r *replyRouter) listen(ctx context.Context, conn *amqp.Connection) error {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a reply channel")

	r.ch = ch

	err = ch.ExchangeDeclare(
		globalReplyExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	queriesQueue, err := r.ch.QueueDeclare(
		r.id,  // name
		true,  // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return err
	}

	err = r.ch.QueueBind(
		queriesQueue.Name,   // queue name
		r.id,                // routing key
		globalReplyExchange, // exchange
		false,
		nil,
	)

	if err != nil {
		return err
	}

	err = r.ch.Qos(r.prefetchCount, 0, false)

	if err != nil {
		return err
	}

	msgs, err := r.ch.Consume(
		queriesQueue.Name, // queue
		r.id,              // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	if err != nil {
		return err
	}

	go r.msgsWorker(ctx, msgs)

	return nil
}

func (r *replyRouter) msgsWorker(ctx context.Context, msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		// Create a copy
		m := msg

		corrId := msg.CorrelationId
		if corrId == "" {
			corrId, ok := msg.Headers[correlationIDHeader]
			if !ok || corrId == "" {
				err := m.Ack(false)
				if err != nil {
					log.Error().Msgf("can not ack/reject msg: %s", err.Error())
					continue
				}

				continue
			}
		}

		if v, ok := r.repliesMap.LoadAndDelete(corrId); ok {
			replyStr := v.(replyStr)
			// Verify if the timeout has already elapsed.
			if !replyStr.timer.Stop() {
				err := m.Ack(false)
				if err != nil {
					log.Error().Msgf("can not ack/reject msg: %s", err.Error())
					continue
				}

				continue
			}

			replyStr.ch <- &Reply{
				Query: replyStr.query,
				Data:  m.Body,
				Err:   nil,
			}

			close(replyStr.ch)

			err := m.Ack(false)
			if err != nil {
				log.Error().Msgf("can not ack/reject msg: %s", err.Error())
				continue
			}

			continue
		}
	}
}

func (r *replyRouter) addReplyToListen(query string, correlationId string) chan *Reply {
	ch := make(chan *Reply, 1)

	timer := time.AfterFunc(r.timeout, func() {
		r.cleanReply(correlationId)
	})

	r.repliesMap.Store(correlationId, replyStr{
		query: query,
		ch:    ch,
		timer: timer,
	})

	return ch
}

func (r *replyRouter) cleanReply(correlationId string) {
	v, ok := r.repliesMap.LoadAndDelete(correlationId)
	if !ok {
		return
	}

	replyStr := v.(replyStr)

	replyStr.ch <- &Reply{
		Err: ErrTimeoutReply,
	}

	close(replyStr.ch)
}
