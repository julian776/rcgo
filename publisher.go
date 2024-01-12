package rcgo

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	id         string
	appName    string
	conn       *amqp.Connection
	ch         *amqp.Channel
	configs    *PublisherConfigs
	repliesMap map[string]interface{}
}

func (p *Publisher) Stop() {
	p.conn.Close()
	p.ch.Close()
}

// Returns a new `Publisher` instance
// with the connection and channel set up.
func NewRabbitPublisher(
	configs *PublisherConfigs,
	appName string,
) *Publisher {
	if configs.Url == "" {
		panic("Can not connect to RabbitMQ url is blank")
	}

	conn, err := amqp.Dial(configs.Url)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	return &Publisher{
		id:      fmt.Sprintf("%s.%s", appName, uuid.NewString()),
		conn:    conn,
		ch:      ch,
		appName: appName,
		configs: configs,
	}
}

func (p *Publisher) queueDeclare(queueName string) error {
	_, err := p.ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare a queue: %s", err.Error())
	}

	return nil
}

// PublishCmd publishes a command to a specified app in RabbitMQ
func (p *Publisher) PublishCmd(
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

	correlationId := uuid.New().String()

	body, err := mapToAmqp(
		correlationId,
		p.appName,
		query,
		MsgTypeEvent,
		data,
	)
	if err != nil {
		return fmt.Errorf("command can not be parsed")
	}

	err = p.ch.PublishWithContext(
		ctx,
		globalReplyExchange, // exchange
		buildQueueName(appTarget, queriesQueueSuffix), // routing key
		false, // mandatory
		false, // immediate
		*body,
	)
	if err != nil {
		return err
	}

	responseChannel := make(chan []byte)
	p.repliesMap[correlationId] = responseChannel

	delay := time.NewTimer(time.Second * p.configs.ReplyTimeout)

	select {
	case <-delay.C:
		err = fmt.Errorf("timeout while waiting for reply %s", query)
	case responsePayload := <-responseChannel:
		if string(responsePayload) == "" || string(responsePayload) == "null" {
			err = fmt.Errorf("data not found [%s]", query)

			break
		}

		err = json.Unmarshal(responsePayload, res)

		if err != nil {
			return err
		}

		if !delay.Stop() {
			<-delay.C
		}
	}

	delete(p.repliesMap, correlationId)

	return err
}
