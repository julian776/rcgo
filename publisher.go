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

// Returns a new `Publisher` instance
// with the connection and channel set up.
func NewPublisher(
	configs *PublisherConfigs,
	appName string,
) *Publisher {
	if configs.Url == "" {
		panic("Can not connect to RabbitMQ url is blank")
	}

	conn, err := amqp.Dial(configs.Url)
	failOnError(err, "failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "failed to open a channel")

	replyRouter := newReplyRouter(
		conn,
		appName,
		configs.ReplyTimeout.Abs(),
	)

	err = replyRouter.listen()
	failOnError(err, "failed to listen for replies")

	return &Publisher{
		id:          fmt.Sprintf("%s.%s", appName, uuid.NewString()),
		conn:        conn,
		ch:          ch,
		appName:     appName,
		configs:     configs,
		replyRouter: replyRouter,
	}
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

// PublishEvent publishes a event to a specified app in RabbitMQ
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

// PublishCmd publishes a command to a specified app in RabbitMQ
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

	if reply.err != nil {
		if err, ok := reply.err.(*TimeoutReplyError); ok {
			return err
		}
	}

	err = json.Unmarshal(reply.data, res)
	if err != nil {
		return err
	}

	return nil
}

// PublishCmd publishes a command to a specified app in RabbitMQ
func (p *Publisher) RequestReplyC(
	ctx context.Context,
	appTarget string,
	query string,
	data interface{},
) (chan *reply, error) {
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
