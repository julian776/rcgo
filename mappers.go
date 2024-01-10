package rcgo

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Headers struct {
	SourceApplication string `json:"sourceApplication"`
}

type Properties struct {
	AppId     string  `json:"appId"`
	Timestamp int64   `json:"timestamp"`
	Headers   Headers `json:"headers"`
}

// func MapAmqpToMsgs[T *Cmd | *Event | Query](message *amqp.Delivery) (T, error) {
// 	err := json.Unmarshal(message.Body, res)
// 	if err != nil {
// 		return res, fmt.Errorf("error parsing delivery: %s", err.Error())
// 	}
// 	return res, nil
// }

func MapCmdToAmqp(cmd *Cmd, sourceAppName string) (*amqp.Publishing, error) {
	bodyBytes, err := json.Marshal(&cmd)
	if err != nil {
		return &amqp.Publishing{}, err
	}

	p := &amqp.Publishing{
		Headers: map[string]interface{}{
			"sourceApplication": sourceAppName,
		},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		MessageId:       uuid.New().String(),
		Timestamp:       time.Now(),
		AppId:           sourceAppName,
		Body:            bodyBytes,
	}

	return p, nil
}

func MapQueryToAmqp(cmd *Query, sourceAppName string) (*amqp.Publishing, error) {
	bodyBytes, err := json.Marshal(&cmd)
	if err != nil {
		return &amqp.Publishing{}, err
	}

	p := &amqp.Publishing{
		Headers: map[string]interface{}{
			"sourceApplication": sourceAppName,
		},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		MessageId:       uuid.New().String(),
		Timestamp:       time.Now(),
		AppId:           sourceAppName,
		Body:            bodyBytes,
	}

	return p, nil
}
