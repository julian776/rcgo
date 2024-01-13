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
	"github.com/rs/zerolog/log"
)

type Listener struct {
	id      string
	appName string
	conn    *amqp.Connection
	ch      *amqp.Channel
	configs ListenerConfigs

	// Keys are the msg types
	cmdHandlers   map[string]CmdHandlerFunc
	eventHandlers map[string]EventHandlerFunc
	queryHandlers map[string]QueryHandlerFunc
}

func NewListener(
	configs ListenerConfigs,
	appName string,
) *Listener {
	err := setupLogger(configs.Timezone, configs.LogLevel)
	if err != nil {
		failOnError(err, "invalid logger configs")
	}

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
	fmt.Printf("[LISTENER]Stopping %s...\n", l.appName)

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
// Processes each msg using the appropiate
// handlers registered for its type.
// If an unexpected msg is received, it will
// acknowledge it to the server and then ignore it.
// The method blocks until the context is done.
func (l *Listener) Listen(
	ctx context.Context,
) error {
	fmt.Printf("[LISTENER]Starting %s...\n", l.appName)

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
		fmt.Sprintf("%s.%s", l.appName, uuid.NewString()),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Error().Msgf("error consuming commands queue: %s", err.Error())
	}

	go l.cmdsWorker(ctx, cmdsDeliveries)

	eventsQueue, err := bindEvents(l.appName, l.ch, l.eventHandlers)
	if err != nil {
		log.Error().Msgf("error consuming events queue: %s", err.Error())
	}

	eventsDeliveries, err := l.ch.Consume(
		eventsQueue.Name,
		fmt.Sprintf("%s.%s", l.appName, uuid.NewString()),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Error().Msgf("error consuming events queue: %s", err.Error())
	}

	go l.eventsWorker(ctx, eventsDeliveries)

	queriesQueue, err := bindQueries(l.appName, l.ch)
	if err != nil {
		log.Error().Msgf("error consuming events queue: %s", err.Error())
	}

	queriesDeliveries, err := l.ch.Consume(
		queriesQueue.Name,
		fmt.Sprintf("%s.%s", l.appName, uuid.NewString()),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Error().Msgf("error consuming queries queue: %s", err.Error())
	}

	go l.queriesWorker(ctx, queriesDeliveries)

	<-ctx.Done()

	return nil
}

func (l *Listener) cmdsWorker(
	ctx context.Context,
	cMessages <-chan amqp.Delivery,
) {
	log.Info().Msgf("[LISTENER-WORKER] Waiting for %s [%d] types of handlers", MsgTypeCmd.String(), len(l.cmdHandlers))
	for msg := range cMessages {
		// Pass a copy of msg
		go func(msg amqp.Delivery) {
			l.processCmd(ctx, &msg)
		}(msg)
	}
}

// Checks if there is a registered handler for
// the msg type, and if not, it logs a warning
// and ignores the msg.
// If there is a registered handlers, it maps the
// `amqp.Delivery` msg to a domain `msg`
// using a mapper function, and then calls each
// registered handler.
func (l *Listener) processCmd(
	ctx context.Context,
	msg *amqp.Delivery,
) {
	defer defaultRecover(msg)

	cmdBody := &cmdBody{}
	err := json.Unmarshal(msg.Body, cmdBody)
	if err != nil {
		log.Error().Msgf("can not process command %s", err.Error())
	}

	cmd := &Cmd{
		Id:             cmdBody.CmdId,
		Source:         msg.AppId,
		Target:         msg.RoutingKey,
		GenerationTime: msg.Timestamp,
		Type:           cmdBody.Name,
		Data:           cmdBody.Data,
	}

	handler, ok := l.cmdHandlers[cmd.Type]
	if !ok {
		l.handleMsgNoHandlers(msg, cmd.Type)
		return
	}

	err = handler(ctx, cmd)
	if err != nil {
		l.handleErrHandler(msg, cmd.Type, err)
	}

	msg.Ack(false)
}

func (l *Listener) eventsWorker(
	ctx context.Context,
	cMessages <-chan amqp.Delivery,
) {
	log.Info().Msgf("[LISTENER-WORKER] Waiting for %s [%d] types of handlers", MsgTypeEvent.String(), len(l.eventHandlers))
	for msg := range cMessages {
		// Pass a copy of msg
		go func(msg amqp.Delivery) {
			l.processEvent(ctx, &msg)
		}(msg)
	}
}

// processEvent Checks if there is a registered handler for
// the msg type, and if not, it logs a warning
// and ignores the msg.
// If there is a registered handlers, it maps the
// `amqp.Delivery` msg to a domain `msg`
// using a mapper function, and then calls each
// registered handler.
func (l *Listener) processEvent(
	ctx context.Context,
	msg *amqp.Delivery,
) {
	defer defaultRecover(msg)

	eventBody := &eventBody{}
	err := json.Unmarshal(msg.Body, eventBody)
	if err != nil {
		log.Error().Msgf("can not process msg %s", err.Error())
	}

	event := &Event{
		Id:             eventBody.Id,
		Source:         msg.AppId,
		GenerationTime: msg.Timestamp,
		Type:           eventBody.Name,
		Data:           eventBody.Data,
	}

	handler, ok := l.eventHandlers[event.Type]
	if !ok {
		l.handleMsgNoHandlers(msg, event.Type)
		return
	}

	err = handler(ctx, event)
	if err != nil {
		l.handleErrHandler(msg, event.Type, err)
	}

	msg.Ack(false)
}

func (l *Listener) queriesWorker(
	ctx context.Context,
	cMessages <-chan amqp.Delivery,
) {
	log.Info().Msgf("[LISTENER-WORKER] Waiting for %s [%d] types of handlers", MsgTypeQuery.String(), len(l.eventHandlers))
	for msg := range cMessages {
		// Pass a copy of msg
		go func(msg amqp.Delivery) {
			l.processQuery(ctx, &msg)
		}(msg)
	}
}

// processQuery Checks if there is a registered handler for
// the msg type, and if not, it logs a warning
// and ignores the msg.
// If there is a registered handlers, it maps the
// `amqp.Delivery` msg to a domain `msg`
// using a mapper function, and then calls each
// registered handler.
func (l *Listener) processQuery(
	ctx context.Context,
	msg *amqp.Delivery,
) {
	defer defaultRecover(msg)

	queryBody := &queryBody{}
	err := json.Unmarshal(msg.Body, queryBody)
	if err != nil {
		log.Error().Msgf("can not process msg %s", err.Error())
	}

	query := &Query{
		Source:         msg.AppId,
		Target:         msg.RoutingKey,
		GenerationTime: msg.Timestamp,
		Type:           queryBody.Resource,
		Data:           queryBody.Data,
	}

	fmt.Printf("query: %+v", query)

	handler, ok := l.queryHandlers[query.Type]
	if !ok {
		l.handleMsgNoHandlers(msg, query.Type)
		return
	}

	fmt.Println("QQQQQQQQQQQQQQQ")

	res, err := handler(ctx, query)
	if err != nil {
		l.handleErrHandler(msg, query.Type, err)
	}

	fmt.Println("QQQQQQQQQQQQQQQ")

	corrId := msg.CorrelationId
	if corrId == "" {
		corrId, ok := msg.Headers[correlationIDHeader].(string)
		if !ok || corrId == "" {
			msg.Ack(false)
			return
		}
	}

	err = l.publishReply(ctx, msg.ReplyTo, corrId, res)
	if err != nil {
		msg.Reject(true)
	}

	msg.Ack(false)
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
	log.Warn().Msgf("ignoring msg due to no handler registered, msg type [%s]", typ)

	if l.configs.AckIfNoHandlers {
		msg.Ack(false)
	} else {
		time.Sleep(l.configs.DelayOnReject.Abs())
		msg.Reject(true)
	}
}

func (l *Listener) handleErrHandler(msg *amqp.Delivery, typ string, err error) {
	log.Error().Msgf(
		"error in handler for type [%s] while processing command %s",
		typ,
		err.Error(),
	)

	time.Sleep(l.configs.DelayOnReject.Abs())
	msg.Reject(true)
}