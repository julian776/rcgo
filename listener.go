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
	configs *ListenerConfigs

	// Keys are the msg types
	cmdHandlers map[string]CmdHandlerFunc
	// Keys are the msg types
	eventHandlers map[string]EventHandlerFunc
	// Keys are the msg types
	queryHandlers map[string]QueryHandlerFunc
}

// Panics if an invalid config is provided.
func NewListener(
	configs *ListenerConfigs,
	appName string,
) *Listener {
	err := setupLogger(configs.Timezone, configs.LogLevel)
	if err != nil {
		failOnError(err, "invalid logger configs")
	}

	if configs.Url == "" {
		log.Panic().Msg("Can not connect to RabbitMQ url is blank")
	}

	if configs.CmdsWorkers == 0 || configs.EventsWorkers == 0 || configs.QueriesWorkers == 0 {
		log.Panic().Msg("The number of workers is invalid. You need at least one for each type of message.\nIf the intention is to avoid adding a worker when there are no handlers, the listener will handle it, so we coerce here to prevent unexpected bugs with zero workers.")
	}

	return &Listener{
		id:            fmt.Sprintf("%s.%s", appName, uuid.NewString()),
		appName:       appName,
		configs:       configs,
		cmdHandlers:   make(map[string]CmdHandlerFunc),
		eventHandlers: make(map[string]EventHandlerFunc),
		queryHandlers: make(map[string]QueryHandlerFunc),
	}
}

// Stop closes the connection with the RabbitMQ server.
func (l *Listener) Stop() error {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, 5*time.Second)

	return l.StopWithContext(ctx)
}

// StopWithContext closes the connection with the RabbitMQ
// server using the specified context.
func (l *Listener) StopWithContext(ctx context.Context) error {
	fmt.Printf("[LISTENER] Stopping %s...\n", l.appName)

	if l.conn == nil {
		return nil
	}

	c := make(chan error)

	go func() {
		c <- l.conn.Close()
	}()

	select {
	case <-ctx.Done():
		return errors.New("ctx expired while stopping listener")
	case err := <-c:
		return err
	}
}

// Add a command handler. If the command type of the message
// is already registered, it will override.
func (l *Listener) AddCommandHandler(
	typeMessage string,
	handler CmdHandlerFunc,
) {
	l.cmdHandlers[typeMessage] = handler
}

// Add a event handler. If the event type of the message
// is already registered, it will override.
func (l *Listener) AddEventHandler(
	typeMessage string,
	handler EventHandlerFunc,
) {
	l.eventHandlers[typeMessage] = handler
}

// Add a query handler. If the query type of the message
// is already registered, it will override.
func (l *Listener) AddQueryHandler(
	typeMessage string,
	handler QueryHandlerFunc,
) {
	l.queryHandlers[typeMessage] = handler
}

// Listen establishes a connection with the RabbitMQ
// server and initiates message consumption for each
// specific type. A consumer is opened only if there
// is a registered handler for that type.
// The method blocks until the context is done.
func (l *Listener) Listen(
	ctx context.Context,
) error {
	fmt.Printf("[LISTENER] Starting %s...\n", l.appName)

	formattedUrl := strings.Replace(l.configs.Url, "\r", "", -1)

	go func() {
		for {
			l.setUpConn(formattedUrl)

			ctx, cancel := context.WithCancel(ctx)
			go l.consume(ctx)

			c := l.conn.NotifyClose(make(chan *amqp.Error))

			err := <-c
			// We need to check for errors because a graceful shutdown returns nil.
			if err == nil {
				cancel()
				break
			}

			// Cancel the last consumption to initiate another cycle with a new one.
			cancel()
			fmt.Printf("[LISTENER] Connection closed by error: %s. reconnecting...\n", err.Error())
		}
	}()

	return nil
}

func (l *Listener) setUpConn(url string) {
	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")

	l.conn = conn

	ch, err := l.conn.Channel()
	failOnError(err, "Failed to open a channel")

	l.ch = ch
}

func (l *Listener) consume(
	ctx context.Context,
) error {
	err := declareExchanges(l.ch)
	if err != nil {
		return fmt.Errorf("error declaring exchanges: %s", err.Error())
	}

	err = l.ch.Qos(l.configs.PrefetchCount, 0, false)
	if err != nil {
		return err
	}

	cmdsQueue, err := bindCommands(l.appName, l.ch)
	if err != nil {
		return fmt.Errorf("error consuming commands queue: %s", err.Error())
	}

	if len(l.cmdHandlers) > 0 {
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
	}

	eventsQueue, err := bindEvents(l.appName, l.ch, l.eventHandlers)
	if err != nil {
		log.Error().Msgf("error consuming events queue: %s", err.Error())
	}

	if len(l.eventHandlers) > 0 {
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
	}

	queriesQueue, err := bindQueries(l.appName, l.ch)
	if err != nil {
		log.Error().Msgf("error consuming events queue: %s", err.Error())
	}

	if len(l.queryHandlers) > 0 {
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
	}

	<-ctx.Done()

	return nil
}

func (l *Listener) cmdsWorker(
	ctx context.Context,
	cMessages <-chan amqp.Delivery,
) {
	log.Info().Msgf("[LISTENER-WORKER] Waiting for %s [%d] types of handlers", MsgTypeCmd.String(), len(l.cmdHandlers))
	for i := 0; i <= l.configs.CmdsWorkers; i++ {
		go func() {
			for msg := range cMessages {
				// Pass a copy of msg
				m := msg
				l.processCmd(ctx, &m)
			}
		}()
	}
}

func (l *Listener) processCmd(
	ctx context.Context,
	msg *amqp.Delivery,
) {
	defer defaultRecover(msg, l.configs.DelayOnReject)

	cmdBody := &cmdBody{}
	err := json.Unmarshal(msg.Body, cmdBody)
	if err != nil {
		l.rejectMsgWithLog(msg, false, "can not process command %s", err.Error())
		return
	}

	cmd := &Cmd{
		Id:             cmdBody.CmdId,
		Source:         msg.AppId,
		Target:         msg.RoutingKey,
		GenerationTime: msg.Timestamp,
		Type:           cmdBody.Name,
		Data:           cmdBody.Data.(map[string]interface{}),
	}

	handler, ok := l.cmdHandlers[cmd.Type]
	if !ok {
		l.handleMsgNoHandlers(msg, cmd.Type)
		return
	}

	err = handler(ctx, cmd)
	if err != nil {
		l.handleErrHandler(msg, cmd.Type, err)
		return
	}

	err = msg.Ack(false)
	if err != nil {
		log.Error().Msgf("can not ack/reject msg: %s", err.Error())
		return
	}
}

func (l *Listener) eventsWorker(
	ctx context.Context,
	cMessages <-chan amqp.Delivery,
) {
	log.Info().Msgf("[LISTENER-WORKER] Waiting for %s [%d] types of handlers", MsgTypeEvent.String(), len(l.eventHandlers))
	for i := 0; i <= l.configs.EventsWorkers; i++ {
		go func() {
			for msg := range cMessages {
				// Pass a copy of msg
				m := msg
				l.processEvent(ctx, &m)
			}
		}()
	}
}

func (l *Listener) processEvent(
	ctx context.Context,
	msg *amqp.Delivery,
) {
	defer defaultRecover(msg, l.configs.DelayOnReject)

	eventBody := &eventBody{}
	err := json.Unmarshal(msg.Body, eventBody)
	if err != nil {
		l.rejectMsgWithLog(msg, false, "can not process msg %s", err.Error())
		return
	}

	event := &Event{
		Id:             eventBody.Id,
		Source:         msg.AppId,
		GenerationTime: msg.Timestamp,
		Type:           eventBody.Name,
		Data:           eventBody.Data.(map[string]interface{}),
	}

	handler, ok := l.eventHandlers[event.Type]
	if !ok {
		l.handleMsgNoHandlers(msg, event.Type)
		return
	}

	err = handler(ctx, event)
	if err != nil {
		l.handleErrHandler(msg, event.Type, err)
		return
	}

	err = msg.Ack(false)
	if err != nil {
		log.Error().Msgf("can not ack/reject msg: %s", err.Error())
		return
	}
}

func (l *Listener) queriesWorker(
	ctx context.Context,
	cMessages <-chan amqp.Delivery,
) {
	log.Info().Msgf("[LISTENER-WORKER] Waiting for %s [%d] types of handlers", MsgTypeQuery.String(), len(l.queryHandlers))
	for i := 0; i <= l.configs.QueriesWorkers; i++ {
		go func() {
			for msg := range cMessages {
				// Pass a copy of msg
				m := msg
				l.processQuery(ctx, &m)
			}
		}()
	}
}

func (l *Listener) processQuery(
	ctx context.Context,
	msg *amqp.Delivery,
) {
	defer defaultRecover(msg, l.configs.DelayOnReject)

	queryBody := &queryBody{}
	err := json.Unmarshal(msg.Body, queryBody)
	if err != nil {
		l.rejectMsgWithLog(msg, false, "can not process msg %s", err.Error())
		return
	}

	query := &Query{
		Source:         msg.AppId,
		Target:         msg.RoutingKey,
		GenerationTime: msg.Timestamp,
		Type:           queryBody.Resource,
		Data:           queryBody.Data.(map[string]interface{}),
	}

	corrId := msg.CorrelationId
	if corrId == "" {
		corrId, ok := msg.Headers[correlationIDHeader].(string)
		if !ok || corrId == "" {
			l.rejectMsgWithLog(msg, false, "correlationID not found. Can not reply")
			return
		}
	}

	handler, ok := l.queryHandlers[query.Type]
	if !ok {
		l.handleMsgNoHandlers(msg, query.Type)
		return
	}

	res, err := handler(ctx, query)
	if err != nil {
		l.handleErrHandler(msg, query.Type, err)
		return
	}

	err = l.publishReply(ctx, msg.ReplyTo, corrId, res)
	if err != nil {
		l.rejectMsgWithLog(msg, true, "error while publishing reply %s", err.Error())
		return
	}

	err = msg.Ack(false)
	if err != nil {
		log.Error().Msgf("can not ack/reject msg: %s", err.Error())
		return
	}
}

func (l *Listener) publishReply(
	ctx context.Context,
	replyTo,
	correlationID string,
	body interface{},
) error {
	d, err := json.Marshal(body)
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
				sourceAppHeader:     l.appName,
				correlationIDHeader: correlationID,
			},
			ContentType:   "application/json",
			CorrelationId: correlationID,
			Body:          d,
		})

	if err != nil {
		return err
	}

	return nil
}

func (l *Listener) handleMsgNoHandlers(msg *amqp.Delivery, typ string) {
	log.Warn().Msgf("ignoring msg due to no handler registered, msg type [%s]", typ)

	if l.configs.AckIfNoHandlers {
		err := msg.Ack(false)
		if err != nil {
			log.Error().Msgf("can not ack/reject msg: %s", err.Error())
			return
		}

		return
	}

	l.rejectMsg(msg, true)
}

func (l *Listener) handleErrHandler(msg *amqp.Delivery, typ string, err error) {
	l.rejectMsgWithLog(
		msg,
		true,
		"error in handler for type [%s] while processing msg %s",
		typ,
		err.Error(),
	)
}

// rejectMsgWithLog wraps [rcgo.rejectMsg]
// and adds a log error.
func (l *Listener) rejectMsgWithLog(msg *amqp.Delivery, requeue bool, format string, v ...interface{}) {
	log.Error().Msgf(format, v...)
	l.rejectMsg(msg, requeue)
}

// rejectMsg aims to establish a standardized
// method for rejecting messages while utilizing
// user-defined configurations.
func (l *Listener) rejectMsg(msg *amqp.Delivery, requeue bool) {
	time.Sleep(l.configs.DelayOnReject.Abs())
	err := msg.Reject(requeue)
	if err != nil {
		log.Error().Msgf("can not ack/reject msg: %s", err.Error())
		return
	}
}
