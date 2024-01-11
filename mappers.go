package rcgo

import (
	"encoding/json"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func mapToAmqp(
	id string,
	sourceAppName string,
	msgName string,
	typ MsgType,
	data interface{},
) (*amqp.Publishing, error) {
	var body interface{}

	p := &amqp.Publishing{
		Headers: map[string]interface{}{
			"sourceApplication": sourceAppName,
		},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		MessageId:       id,
		Timestamp:       time.Now(),
		AppId:           sourceAppName,
	}

	switch typ {
	case MsgTypeCmd:
		body = cmdBody{CmdId: id, Name: msgName, Data: data}
	case MsgTypeEvent:
		body = eventBody{Id: id, Name: msgName, Data: data}
	case MsgTypeQuery:
		body = queryBody{Resource: msgName, Data: data}

		p.Headers["x-reply_id"] = id
		p.Headers["x-correlation-id"] = id
		p.Headers["x-serveQuery-id"] = msgName

		p.ReplyTo = sourceAppName
	}

	d, err := json.Marshal(&body)
	if err != nil {
		return &amqp.Publishing{}, err
	}

	p.Body = d

	return p, nil
}
