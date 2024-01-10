package rcgo

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitPublisher struct {
	logger  Logger
	Conn    *amqp.Connection
	Ch      *amqp.Channel
	appName string
	configs *PublisherConfigs
}

func (c *RabbitPublisher) Stop() {
	c.Conn.Close()
	c.Ch.Close()
}

// Returns a new `RabbitPublisher` instance
// with the connection and channel set up.
func NewRabbitPublisher(
	logger Logger,
	configs *PublisherConfigs,
	appName string,
) *RabbitPublisher {
	if configs.Url == "" {
		logger.Errorf("Can not connect to RabbitMQ url is blank")
		return &RabbitPublisher{}
	}

	conn, err := amqp.Dial(configs.Url)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	return &RabbitPublisher{
		logger,
		conn,
		ch,
		appName,
		configs,
	}
}

func (c *RabbitPublisher) QueueDeclare(queueName string) error {
	_, err := c.Ch.QueueDeclare(
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

// Publishes a message to a specified queue in RabbitMQ.
// It maps the message to a byte array using the
// `mappers.MapStructToMessage` function and then publishes
// the message.
func (c *RabbitPublisher) PublishCmd(
	ctx context.Context,
	appName string,
	cmd *Cmd,
) error {
	if c.appName == "" {
		return fmt.Errorf("error: publisher not correctly setup. You already run setup()?")
	}

	body, err := MapCmdToAmqp(
		cmd,
		c.appName,
	)
	if err != nil {
		return fmt.Errorf("command can not be parsed")
	}

	err = c.Ch.PublishWithContext(
		ctx,
		directMessagesExchange, // exchange
		appName,                // routing key
		false,                  // mandatory
		false,                  // immediate
		*body,
	)

	return err
}
