package helper

// Client is the common interface for all client-side operations.
type Client interface {
	Run() error
}

// createClientImpl is a factory function that creates the appropriate client implementation.
func createClientImpl(config *Config) Client {
	isConcurrentReceive := config.Direction == DirectionModeReceive &&
		len(config.Hosts) > config.NumDests &&
		config.NumDests > 0

	if isConcurrentReceive {
		return NewConcurrentClient(config)
	}

	return NewOneTimeClient(config)
}
