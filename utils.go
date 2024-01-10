package rcgo

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func bindCommands(appName string, ch *amqp.Channel) (*amqp.Queue, error) {
	queue, err := ch.QueueDeclare(buildQueueName(appName, commandsQueueSuffix), true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	err = ch.QueueBind(queue.Name, appName, directMessagesExchange, false, nil)
	if err != nil {
		return nil, err
	}

	return &queue, nil
}

func bindEvents(appName string, ch *amqp.Channel, eventHandlers map[string]EventHandlerFunc) (*amqp.Queue, error) {
	queue, err := ch.QueueDeclare(buildQueueName(appName, eventsQueueSuffix), true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	for event := range eventHandlers {
		err = ch.QueueBind(queue.Name, event, eventsExchange, false, nil)
		if err != nil {
			return nil, err
		}
	}

	return &queue, nil
}

func buildQueueName(appName, suffix string) string {
	return fmt.Sprintf("%s.%s", appName, suffix)
}

func defaultRecover(logger Logger, message *amqp.Delivery) {
	if err := recover(); err != nil {
		logger.Errorf("recover from panic on listener error: %s", err.(error).Error())
		message.Reject(true)
	}
}
