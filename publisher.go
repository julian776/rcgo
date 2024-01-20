package rcgo

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	id          string
	appName     string
	conn        *amqp.Connection
	ch          *amqp.Channel
	configs     *PublisherConfigs
	replyRouter *replyRouter
}

func (p *Publisher) Stop() {
	p.conn.Close()
	p.ch.Close()
}

func NewPublisher(
	configs *PublisherConfigs,
	appName string,
) *Publisher {
	if configs.Url == "" {
		panic("Can not connect to RabbitMQ url is blank")
	}

	replyRouter := newReplyRouter(
		appName,
		configs.ReplyTimeout.Abs(),
	)

	err = replyRouter.listen(conn)
	failOnError(err, "failed to listen for replies")

	return &Publisher{
		id:          fmt.Sprintf("%s.%s", appName, uuid.NewString()),
		appName:     appName,
		configs:     configs,
		replyRouter: replyRouter,
	}
}

// Start establishes a connection to the RabbitMQ server.
// It should be invoked before publishing any messages.
func (p *Publisher) Start(
	ctx context.Context,
) {
	conn, err := amqp.Dial(p.configs.Url)
	failOnError(err, "failed to connect to RabbitMQ")

	p.conn = conn

	ch, err := conn.Channel()
	failOnError(err, "failed to open a channel")

	p.ch = ch

	err = p.replyRouter.listen(conn)
	failOnError(err, "failed to listen for replies")
}

// SendCmd publishes a command to a specified app in RabbitMQ
func (p *Publisher) SendCmd(
	ctx context.Context,
	appTarget string,
	cmd string,
	data interface{},
) error {
	body, err := mapToAmqp(
		uuid.NewString(),
		p.appName,
		cmd,
		MsgTypeCmd,
		data,
	)
	if err != nil {
		return fmt.Errorf("command can not be parsed")
	}

	err = p.ch.PublishWithContext(
		ctx,
		directMessagesExchange, // exchange
		appTarget,              // routing key
		false,                  // mandatory
		false,                  // immediate
		*body,
	)

	return err
}

// PublishEvent publishes an event to the message broker.
func (p *Publisher) PublishEvent(
	ctx context.Context,
	event string,
	data interface{},
) error {
	body, err := mapToAmqp(
		uuid.NewString(),
		p.appName,
		event,
		MsgTypeEvent,
		data,
	)
	if err != nil {
		return fmt.Errorf("event can not be parsed")
	}

	err = p.ch.PublishWithContext(
		ctx,
		eventsExchange, // exchange
		event,          // routing key
		false,          // mandatory
		false,          // immediate
		*body,
	)

	return err
}

// RequestReply function serves as a wrapper
// for [rcgo.RequestReplyC] managing response
// handling and returning the reply through
// the `res` parameter.
func (p *Publisher) RequestReply(
	ctx context.Context,
	appTarget string,
	query string,
	data interface{},
	res interface{},
) error {
	if reflect.ValueOf(res).Kind() != reflect.Pointer {
		return fmt.Errorf("res value must be a pointer")
	}

	resCh, err := p.RequestReplyC(ctx, appTarget, query, data)
	if err != nil {
		return err
	}

	reply := <-resCh

	if reply.Err != nil {
		if reply.Err == ErrTimeoutReply {
			return err
		}
	}

	err = json.Unmarshal(reply.Data, res)
	if err != nil {
		return err
	}

	return nil
}

// RequestReplyC sends a request to a specific
// application target, expecting a reply and
// providing a channel to receive the reply
// asynchronously.
//
// When using the returned reply channel, ensure
// to handle closure events.
// The channel will be closed when a reply is
// received or when a timeout occurs.
// Listen for the [rcgo.ErrTimeoutReply]
// error on the reply to appropriately handle it.
//
// Example:
//
//	reply := <-resCh
//
//	if reply.Err != nil {
//		if reply.Err == ErrTimeoutReply {
//			return err
//		}
//	 }
//
//	err = json.Unmarshal(reply.data, res)
//	if err != nil {
//		return err
//	}
func (p *Publisher) RequestReplyC(
	ctx context.Context,
	appTarget string,
	query string,
	data interface{},
) (chan *Reply, error) {
	correlationId := uuid.NewString()

	body, err := mapToAmqp(
		correlationId,
		p.replyRouter.id,
		query,
		MsgTypeQuery,
		data,
	)
	if err != nil {
		return nil, fmt.Errorf("command can not be parsed")
	}

	err = p.ch.PublishWithContext(
		ctx,
		directMessagesExchange, // exchange
		buildQueueName(appTarget, queriesQueueSuffix), // routing key
		false, // mandatory
		false, // immediate
		*body,
	)
	if err != nil {
		return nil, err
	}

	return p.replyRouter.addReplyToListen(query, correlationId), nil
}
