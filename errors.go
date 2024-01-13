package rcgo

type TimeoutReplyError struct {
	msg string
}

func (e *TimeoutReplyError) Error() string {
	if e.msg == "" {
		e.msg = "timeout while waiting for a reply"
	}

	return e.msg
}

//////////////////////////////////////

// type NoContentReplyError struct {
// 	msg string
// }

// func (e *TimeoutReplyError) Error() string {
// 	if e.msg == "" {
// 		e.msg = "timeout while waiting for a response."
// 	}

// 	return e.msg
// }
