package rcgo

import "time"

type ListenerConfigs struct {
	Url string

	// Acknowledge the server as if
	// the message was successfully
	// processed. Defaults to `false`.
	AckIfNoHandlers bool

	// Time to delay when rejecting
	// messages to the server, defaults to zero.
	DelayOnReject time.Duration
}

type PublisherConfigs struct {
	Url          string
	ReplyTimeout time.Duration
}
