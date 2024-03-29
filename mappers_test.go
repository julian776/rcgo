package rcgo

import (
	"encoding/json"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func Test_mapToAmqp_Cmd(t *testing.T) {
	const (
		id            = "1"
		sourceAppName = "test"
		msgName       = "anyCmd"
		typ           = MsgTypeCmd
	)
	data := []byte("")

	got, err := mapToAmqp(id, sourceAppName, msgName, typ, data, Options{})
	assert.Nil(t, err)

	now := time.Now()

	assert.InEpsilon(t, got.Timestamp.Unix(), now.Unix(), 0.01)

	// Clean timestamp after assert it to simplify
	got.Timestamp = now

	body, _ := json.Marshal(&cmdBody{CmdId: id, Name: msgName, Data: data})

	want := &amqp.Publishing{
		Headers: map[string]interface{}{
			"sourceApplication": sourceAppName,
		},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		MessageId:       id,
		Timestamp:       now,
		AppId:           sourceAppName,
		Body:            body,
	}

	assert.Equal(t, want, got)
}

func Test_mapToAmqp_Event(t *testing.T) {
	const (
		id            = "1"
		sourceAppName = "test"
		msgName       = "anyEvent"
		typ           = MsgTypeEvent
	)
	data := []byte("")

	got, err := mapToAmqp(id, sourceAppName, msgName, typ, data, Options{})
	assert.Nil(t, err)

	now := time.Now()

	assert.InEpsilon(t, got.Timestamp.Unix(), now.Unix(), 0.01)

	// Clean timestamp after assert it to simplify
	got.Timestamp = now

	body, _ := json.Marshal(&eventBody{Id: id, Name: msgName, Data: data})

	want := &amqp.Publishing{
		Headers: map[string]interface{}{
			"sourceApplication": sourceAppName,
		},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		MessageId:       id,
		Timestamp:       now,
		AppId:           sourceAppName,
		Body:            body,
	}

	assert.Equal(t, want, got)
}

func Test_mapToAmqp_Query(t *testing.T) {
	const (
		id            = "1"
		sourceAppName = "test"
		msgName       = "anyQuery"
		typ           = MsgTypeQuery
	)
	data := []byte("")

	got, err := mapToAmqp(id, sourceAppName, msgName, typ, data, Options{})
	assert.Nil(t, err)

	now := time.Now()

	assert.InEpsilon(t, got.Timestamp.Unix(), now.Unix(), 0.01)

	// Clean timestamp after assert it to simplify
	got.Timestamp = now

	body, _ := json.Marshal(&queryBody{Resource: msgName, Data: data})

	want := &amqp.Publishing{
		Headers: map[string]interface{}{
			"sourceApplication": sourceAppName,
			correlationIDHeader: id,
			serveQueryIDHeader:  msgName,
			replyIDHeader:       id,
		},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		MessageId:       id,
		Timestamp:       now,
		AppId:           sourceAppName,
		ReplyTo:         sourceAppName,
		CorrelationId:   id,
		Body:            body,
	}

	assert.Equal(t, want, got)
}

func Test_mapToAmqp_OptionsSetted(t *testing.T) {
	const (
		id            = "1"
		sourceAppName = "test"
		msgName       = "anyQuery"
		typ           = MsgTypeQuery
	)
	data := []byte("")

	opts := Options{
		Expiration: "10000",
	}

	got, err := mapToAmqp(id, sourceAppName, msgName, typ, data, opts)
	assert.Nil(t, err)

	now := time.Now()

	assert.InEpsilon(t, got.Timestamp.Unix(), now.Unix(), 0.01)

	// Clean timestamp after assert it to simplify
	got.Timestamp = now

	body, _ := json.Marshal(&queryBody{Resource: msgName, Data: data})

	want := &amqp.Publishing{
		Headers: map[string]interface{}{
			"sourceApplication": sourceAppName,
			correlationIDHeader: id,
			serveQueryIDHeader:  msgName,
			replyIDHeader:       id,
		},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		MessageId:       id,
		Timestamp:       now,
		AppId:           sourceAppName,
		ReplyTo:         sourceAppName,
		CorrelationId:   id,
		Body:            body,
		Expiration:      "10000",
	}

	assert.Equal(t, want, got)
}
