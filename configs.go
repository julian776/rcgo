package rcgo

import "time"

type ListenerConfigs struct {
	Url string

	// Acknowledge the server as if
	// the message was successfully
	// processed. Defaults to `false`.
	AckIfNoHandlers bool
}

type PublisherConfigs struct {
	Url          string
	ReplyTimeout time.Duration
}
