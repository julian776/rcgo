package rcgo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	id          string
	appName     string
	isStopped   bool
	conn        *amqp.Connection
	ch          *amqp.Channel
	configs     *PublisherConfigs
	replyRouter *replyRouter
}

// Stop closes the connection with the RabbitMQ server.
func (p *Publisher) Stop() error {
	p.isStopped = true
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, 5*time.Second)

	return p.StopWithContext(ctx)
}

// StopWithContext closes the connection with the RabbitMQ
// server using the specified context.
func (p *Publisher) StopWithContext(ctx context.Context) error {
	p.isStopped = true
	fmt.Printf("[PUBLISHER] Stopping %s...\n", p.appName)

	if p.conn == nil {
		return nil
	}

	cErr := make(chan error)
	cDone := make(chan struct{})
	doneCount := 2

	go func() {
		err := p.conn.Close()
		if err != nil {
			cErr <- err
		}

		cDone <- struct{}{}
	}()

	go func() {
		err := p.replyRouter.stop(ctx)
		if err != nil {
			cErr <- err
		}

		cDone <- struct{}{}
	}()

	for {
		select {
		case <-ctx.Done():
			return errors.New("error: ctx expired while stopping publisher")
		case err := <-cErr:
			return err
		case <-cDone:
			doneCount--
			if doneCount <= 0 {
				return nil
			}
		}
	}
}

// Panics if an invalid config is provided.
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
		configs.PrefetchCount,
	)

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
) error {
	p.isStopped = false
	fmt.Printf("[PUBLISHER] Starting %s...\n", p.appName)

	conn, err := amqp.Dial(p.configs.Url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %s", err.Error())
	}

	p.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %s", err.Error())
	}

	p.ch = ch

	err = p.replyRouter.listen(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to listen for replies: %s", err.Error())
	}

	return nil
}

// SendCmd publishes a command to a specified app in RabbitMQ
func (p *Publisher) SendCmd(
	ctx context.Context,
	appTarget string,
	cmd string,
	data interface{},
	options ...Options,
) error {
	if p.isStopped {
		return ErrPublisherStopped
	}

	err := p.validateConn(ctx)
	if err != nil {
		return err
	}

	opts := p.firstOrDefaultOptions(options)

	body, err := mapToAmqp(
		uuid.NewString(),
		p.appName,
		cmd,
		MsgTypeCmd,
		data,
		opts,
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
	options ...Options,
) error {
	if p.isStopped {
		return ErrPublisherStopped
	}

	err := p.validateConn(ctx)
	if err != nil {
		return err
	}

	opts := p.firstOrDefaultOptions(options)

	body, err := mapToAmqp(
		uuid.NewString(),
		p.appName,
		event,
		MsgTypeEvent,
		data,
		opts,
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
	options ...Options,
) error {
	if p.isStopped {
		return ErrPublisherStopped
	}

	if reflect.ValueOf(res).Kind() != reflect.Pointer {
		return fmt.Errorf("res value must be a pointer")
	}

	resCh, err := p.RequestReplyC(ctx, appTarget, query, data, options...)
	if err != nil {
		return err
	}

	reply := <-resCh

	if reply.Err != nil {
		return reply.Err
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
// Listen for the [rcgo.ErrTimeoutReply] or
// [rcgo.ErrCanceledReply] errors on the reply
// to appropriately handle it.
//
// By default, the expiration is set to the configs
// ReplyTimeout plus one second.
// If an expiration is provided in the Options,
// the smaller value will be set.
//
// Example:
//
//	resCh, err := p.RequestReplyC(ctx, "anyListener", "anyListener.employees", qdata)
//	if err != nil {
//		return err
//	}
//
//	reply := <-resCh
//
//	if reply.Err != nil {
//		if reply.Err == ErrTimeoutReply {
//			// Solely for demonstrating the handling of a timeout error.
//			return err
//		}
//		return err
//	 }
//
//	res := make(map[string]interface{})
//
//	err = json.Unmarshal(reply.data, &res)
//	if err != nil {
//		return err
//	}
func (p *Publisher) RequestReplyC(
	ctx context.Context,
	appTarget string,
	query string,
	data interface{},
	options ...Options,
) (chan *Reply, error) {
	if p.isStopped {
		return nil, ErrPublisherStopped
	}

	err := p.validateConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("error when request reply %s", err.Error())
	}

	correlationId := uuid.NewString()

	opts := p.firstOrDefaultOptions(options)

	rt := fmt.Sprint(p.configs.ReplyTimeout.Milliseconds())
	if opts.Expiration == "" {
		opts.Expiration = rt
	} else {
		ex1, err := strconv.ParseInt(opts.Expiration, 10, 64)
		if err != nil {
			opts.Expiration = rt
		}

		if p.configs.ReplyTimeout < time.Duration(ex1) {
			opts.Expiration = rt
		}
	}

	body, err := mapToAmqp(
		correlationId,
		p.replyRouter.id,
		query,
		MsgTypeQuery,
		data,
		opts,
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

// validateConn validates the connection, and if an error,
// restart the publisher.
func (p *Publisher) validateConn(ctx context.Context) error {
	if !p.isStopped && (p.ch == nil || p.ch.IsClosed()) {
		return p.Start(ctx)
	}

	return nil
}

func (p *Publisher) firstOrDefaultOptions(options []Options) Options {
	opts := Options{}
	if len(options) > 0 {
		opts = options[0]
	}

	return opts
}
