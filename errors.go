package rcgo

import "errors"

var (
	ErrTimeoutReply = errors.New("timeout while waiting for a reply")
)
