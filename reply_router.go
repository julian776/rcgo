package rcgo

import (
	"fmt"
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
type repliesMap map[interface{}]replyStr

type replyRouter struct {
	id         string
	ch         *amqp.Channel
	repliesMap repliesMap
	timeout    time.Duration
}

func newReplyRouter(
	appName string,
	timeout time.Duration,
) *replyRouter {
	if timeout < time.Millisecond*50 {
		log.Panic().Msg("Your timeout is too short, please consider give enough timeout to your replies.")
	}

	return &replyRouter{
		id:         fmt.Sprintf("%s.%s", appName, uuid.NewString()),
		repliesMap: make(repliesMap),
		timeout:    timeout,
	}
}

func (r *replyRouter) listen(conn *amqp.Connection) error {
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

	err = r.ch.Qos(
		15,    // prefetch count
		0,     // prefetch size
		false, // global
	)

	if err != nil {
		return err
	}

	msgs, err := r.ch.Consume(
		queriesQueue.Name, // queue
		r.id,              // consumer
		true,              // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)

	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			// Create a copy
			m := msg

			corrId := msg.CorrelationId
			if corrId == "" {
				corrId, ok := msg.Headers[correlationIDHeader]
				if !ok || corrId == "" {
					m.Ack(false)
					continue
				}
			}

			if replyStr, ok := r.repliesMap[corrId]; ok {
				// Verify if the timeout has already elapsed.
				if !replyStr.timer.Stop() {
					m.Ack(false)
					continue
				}

				replyStr.ch <- &Reply{
					Query: replyStr.query,
					Data:  m.Body,
					Err:   nil,
				}

				close(replyStr.ch)

				delete(r.repliesMap, corrId)

				m.Ack(false)
			}
		}
	}()

	return nil
}

func (r *replyRouter) addReplyToListen(query string, correlationId string) chan *Reply {
	ch := make(chan *Reply)

	timer := time.AfterFunc(r.timeout, func() {
		r.cleanReply(correlationId)
	})

	r.repliesMap[correlationId] = replyStr{
		query: query,
		ch:    ch,
		timer: timer,
	}

	return ch
}

func (r *replyRouter) cleanReply(correlationId string) {
	replyStr, ok := r.repliesMap[correlationId]
	if !ok {
		return
	}

	replyStr.ch <- &Reply{
		Err: ErrTimeoutReply,
	}

	close(replyStr.ch)

	delete(r.repliesMap, correlationId)
}
