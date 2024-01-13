package rcgo

const (
	globalReplyExchange    = "globalReply"
	eventsExchange         = "domainEvents"
	directMessagesExchange = "directMessages"
	eventsQueueSuffix      = "subsEvents"
	commandsQueueSuffix    = "commands"
	queriesQueueSuffix     = "query"

	correlationIDHeader = "x-correlation-id"
	sourceAppHeader     = "sourceApplication"
	replyIDHeader       = "x-reply_id"
	serveQueryIDHeader  = "x-serveQuery-id"
)
