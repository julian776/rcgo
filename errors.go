package rcgo

import "errors"

var (
	ErrTimeoutReply     = errors.New("timeout while waiting for a reply")
	ErrCanceledReply    = errors.New("context canceled while waiting for a reply")
	ErrPublisherStopped = errors.New("publisher stopped and is unable to send any messages to the server")
)
