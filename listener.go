package rcgo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Listener struct {
	id      string
	appName string
	logger  Logger
	conn    *amqp.Connection
	ch      *amqp.Channel
	configs ListenerConfigs

	// Keys are the message types
	cmdHandlers   map[string]CmdHandlerFunc
	eventHandlers map[string]EventHandlerFunc
	queryHandlers map[string]QueryHandlerFunc
}

func NewListener(
	logger Logger,
	configs ListenerConfigs,
	appName string,
) *Listener {
	if configs.Url == "" {
		panic("Can not connect to RabbitMQ url is blank")
	}

	formattedUrl := strings.Replace(configs.Url, "\r", "", -1)

	conn, err := amqp.Dial(formattedUrl)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	return &Listener{
		id:            fmt.Sprintf("%s.%s", appName, uuid.NewString()),
		appName:       appName,
		logger:        logger,
		conn:          conn,
		ch:            ch,
		configs:       configs,
		cmdHandlers:   make(map[string]CmdHandlerFunc),
		eventHandlers: make(map[string]EventHandlerFunc),
		queryHandlers: make(map[string]QueryHandlerFunc),
	}
}

func (l *Listener) Stop() error {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, 5*time.Second)

	return l.StopWithContext(ctx)
}

func (l *Listener) StopWithContext(ctx context.Context) error {
	l.logger.Infof("Stopping listener...")
	c := make(chan error)

	go func() {
		c <- l.conn.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return errors.New("error: ctx expired while closing stopping listener")
		case err := <-c:
			return err
		}
	}
}

func (l *Listener) AddCommandHandler(
	typeMessage string,
	handler CmdHandlerFunc,
) {
	l.cmdHandlers[typeMessage] = handler
}

func (l *Listener) AddEventHandler(
	typeMessage string,
	handler EventHandlerFunc,
) {
	l.eventHandlers[typeMessage] = handler
}

func (l *Listener) AddQueryHandler(
	typeMessage string,
	handler QueryHandlerFunc,
) {
	l.queryHandlers[typeMessage] = handler
}

// Listens to the apps added and processes
// the messages received from them.
// Processes each message using the appropiate
// handlers registered for its type.
// If an unexpected message is received, it will
// acknowledge it to the server and then ignore it.
// The method blocks until the context is done.
func (l *Listener) Listen(
	ctx context.Context,
) error {
	err := declareExchanges(l.ch)
	if err != nil {
		return fmt.Errorf("error declaring exchanges: %s", err.Error())
	}

	cmdsQueue, err := bindCommands(l.appName, l.ch)
	if err != nil {
		return fmt.Errorf("error consuming commands queue: %s", err.Error())
	}

	err = l.ch.Qos(15, 0, false)
	if err != nil {
		return err
	}

	cmdsDeliveries, err := l.ch.Consume(
		cmdsQueue.Name,
		l.id,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		l.logger.Errorf("error consuming commands queue: %s", err.Error())
	}

	go l.cmdsWorker(ctx, cmdsDeliveries)

	eventsQueue, err := bindEvents(l.appName, l.ch, l.eventHandlers)
	if err != nil {
		l.logger.Errorf("error consuming events queue: %s", err.Error())
	}

	eventsDeliveries, err := l.ch.Consume(
		eventsQueue.Name,
		l.id,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		l.logger.Errorf("error consuming events queue: %s", err.Error())
	}

	go l.eventsWorker(ctx, eventsDeliveries)

	queriesQueue, err := bindQueries(l.appName, l.ch)
	if err != nil {
		l.logger.Errorf("error consuming events queue: %s", err.Error())
	}

	queriesDeliveries, err := l.ch.Consume(
		queriesQueue.Name,
		l.id,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		l.logger.Errorf("error consuming queries queue: %s", err.Error())
	}

	go l.queriesWorker(ctx, queriesDeliveries)

	<-ctx.Done()

	return nil
}

func (l *Listener) cmdsWorker(
	ctx context.Context,
	cMessages <-chan amqp.Delivery,
) {
	l.logger.Infof("[LISTENER-WORKER] Waiting for %s [%d] types of handlers", MsgTypeCmd.String(), (l.cmdHandlers))
	for message := range cMessages {
		// Pass a copy of msg
		go func(message amqp.Delivery) {
			l.processCmd(ctx, &message)
		}(message)
	}
}

// Checks if there is a registered handler for
// the message type, and if not, it logs a warning
// and ignores the message.
// If there is a registered handlers, it maps the
// `amqp.Delivery` message to a domain `Message`
// using a mapper function, and then calls each
// registered handler.
func (l *Listener) processCmd(
	ctx context.Context,
	message *amqp.Delivery,
) {
	defer defaultRecover(l.logger, message)

	cmd := &Cmd{}
	err := json.Unmarshal(message.Body, cmd)
	if err != nil {
		l.logger.Errorf("can not process command %s", err.Error())
	}

	handler, ok := l.cmdHandlers[cmd.Data.Name]
	if !ok {
		l.handleMsgNoHandlers(message, cmd.Data.Name)
		return
	}

	err = handler(ctx, cmd)
	if err != nil {
		l.handleErrHandler(message, cmd.Data.Name, err)
	}

	message.Ack(false)
}

func (l *Listener) eventsWorker(
	ctx context.Context,
	cMessages <-chan amqp.Delivery,
) {
	l.logger.Infof("[LISTENER-WORKER] Waiting for %s [%d] types of handlers", MsgTypeEvent.String(), len(l.eventHandlers))
	for message := range cMessages {
		// Pass a copy of msg
		go func(message amqp.Delivery) {
			l.processEvent(ctx, &message)
		}(message)
	}
}

// processEvent Checks if there is a registered handler for
// the message type, and if not, it logs a warning
// and ignores the message.
// If there is a registered handlers, it maps the
// `amqp.Delivery` message to a domain `Message`
// using a mapper function, and then calls each
// registered handler.
func (l *Listener) processEvent(
	ctx context.Context,
	message *amqp.Delivery,
) {
	defer defaultRecover(l.logger, message)

	event := &Event{}
	err := json.Unmarshal(message.Body, event)
	if err != nil {
		l.logger.Errorf("can not process message %s", err.Error())
	}

	handler, ok := l.eventHandlers[event.Data.Name]
	if !ok {
		l.handleMsgNoHandlers(message, event.Data.Name)
		return
	}

	err = handler(ctx, event)
	if err != nil {
		l.handleErrHandler(message, event.Data.Name, err)
	}

	message.Ack(false)
}

func (l *Listener) queriesWorker(
	ctx context.Context,
	cMessages <-chan amqp.Delivery,
) {
	l.logger.Infof("[LISTENER-WORKER] Waiting for %s [%d] types of handlers", MsgTypeQuery.String(), len(l.eventHandlers))
	for message := range cMessages {
		// Pass a copy of msg
		go func(message amqp.Delivery) {
			l.processQuery(ctx, &message)
		}(message)
	}
}

// processQuery Checks if there is a registered handler for
// the message type, and if not, it logs a warning
// and ignores the message.
// If there is a registered handlers, it maps the
// `amqp.Delivery` message to a domain `Message`
// using a mapper function, and then calls each
// registered handler.
func (l *Listener) processQuery(
	ctx context.Context,
	message *amqp.Delivery,
) {
	defer defaultRecover(l.logger, message)

	query := &Query{}
	err := json.Unmarshal(message.Body, query)
	if err != nil {
		l.logger.Errorf("can not process message %s", err.Error())
	}

	handler, ok := l.queryHandlers[query.Data.Resource]
	if !ok {
		l.handleMsgNoHandlers(message, query.Data.Resource)
		return
	}

	res, err := handler(ctx, query)
	if err != nil {
		l.handleErrHandler(message, query.Data.Resource, err)
	}

	err = l.publishReply(ctx, message.ReplyTo, message.CorrelationId, res)
	if err != nil {
		message.Reject(true)
	}

	message.Ack(false)
}

func (l *Listener) publishReply(
	ctx context.Context,
	replyTo,
	correlationID string,
	body interface{},
) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	err = l.ch.PublishWithContext(
		ctx,
		globalReplyExchange, // exchange
		replyTo,             // routing key
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			Headers: map[string]interface{}{
				"sourceApplicationHeader": l.appName,
				"correlationIDHeader":     correlationID,
			},
			ContentType:   "application/json",
			CorrelationId: correlationID,
			Body:          data,
		})

	if err != nil {
		return err
	}

	return nil
}

func (l *Listener) handleMsgNoHandlers(msg *amqp.Delivery, typ string) {
	l.logger.Warnf("ignoring message due to no handler registered, message type [%s]", typ)

	if l.configs.AckIfNoHandlers {
		msg.Ack(false)
	} else {
		time.Sleep(l.configs.DelayOnReject.Abs())
		msg.Reject(true)
	}
}

func (l *Listener) handleErrHandler(msg *amqp.Delivery, typ string, err error) {
	l.logger.Errorf(
		"error in handler for type [%s] while processing command %s",
		typ,
		err.Error(),
	)

	time.Sleep(l.configs.DelayOnReject.Abs())
	msg.Reject(true)
}
