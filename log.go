package rcgo

type Logger interface {
	Infof(template string, args ...interface{})

	Warnf(template string, args ...interface{})

	Errorf(template string, args ...interface{})

	Fatalf(template string, args ...interface{})

	// Sync flushes any buffered log entries.
	Sync() error
}
