package rcgo

import (
	"context"
	"time"
)

type MsgType int8

const (
	MsgTypeCmd MsgType = iota
	MsgTypeEvent
	MsgTypeQuery
)

func (t MsgType) String() string {
	switch t {
	case MsgTypeEvent:
		return "event"
	case MsgTypeCmd:
		return "command"
	case MsgTypeQuery:
		return "query"
	}

	return "unknown"
}

type CmdHandlerFunc func(
	context.Context,
	*Cmd,
) error

type EventHandlerFunc func(
	context.Context,
	*Event,
) error

type QueryHandlerFunc func(
	context.Context,
	*Query,
) (interface{}, error)

type Event struct {
	Id             string
	Source         string
	Type           string
	GenerationTime time.Time
	Data           interface{}
}

type eventBody struct {
	Name string      `json:"name"`
	Id   string      `json:"eventId"`
	Data interface{} `json:"data"`
}

type Cmd struct {
	Id             string
	Source         string
	Target         string
	Type           string
	GenerationTime time.Time
	Data           interface{}
}

type cmdBody struct {
	Name  string      `json:"name"`
	CmdId string      `json:"commandId"`
	Data  interface{} `json:"data"`
}

type Query struct {
	Source         string
	Target         string
	Type           string
	GenerationTime time.Time
	Data           interface{}
}

type queryBody struct {
	Resource string      `json:"resource"`
	Data     interface{} `json:"queryData"`
}
