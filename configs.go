package rcgo

import "time"

type ListenerConfigs struct {
	Url string
}

type PublisherConfigs struct {
	Url          string
	ReplyTimeout time.Duration
}
