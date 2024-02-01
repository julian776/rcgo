package rcgo

import "time"

type ListenerConfigs struct {
	Url string

	// The server will deliver that many
	// messages to consumers before
	// acknowledgments are received.
	// 0 means no limit.
	PrefetchCount int

	// Number of cmds workers to spawn
	CmdsWorkers int

	// Number of events workers to spawn
	EventsWorkers int

	// Number of queries workers to spawn
	QueriesWorkers int

	// Acknowledge the server as if
	// the message was successfully
	// processed.
	AckIfNoHandlers bool

	// Time to delay when rejecting
	// messages to the server.
	DelayOnReject time.Duration

	// Timezone to be used on the listener and the logger.
	Timezone *time.Location

	// Zerolog valid level. From `trace` to `panic`.
	// To disable `disabled`
	LogLevel string
}

func NewListenerDefaultConfigs(url string) *ListenerConfigs {
	return &ListenerConfigs{
		Url:             url,
		AckIfNoHandlers: true,
		DelayOnReject:   5 * time.Second,
		Timezone:        time.UTC,
		LogLevel:        "info",
		CmdsWorkers:     5,
		EventsWorkers:   5,
		QueriesWorkers:  5,
		PrefetchCount:   15,
	}
}

type PublisherConfigs struct {
	Url           string
	ReplyTimeout  time.Duration
	PrefetchCount int
}

func NewPublisherDefaultConfigs(url string) *PublisherConfigs {
	return &PublisherConfigs{
		Url:           url,
		ReplyTimeout:  15 * time.Second,
		PrefetchCount: 15,
	}
}
