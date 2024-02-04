package lib

type EventQueue interface {
	GetSendChan() chan<- SinkEvent
	GetRecvChan() <-chan SourceEvent
	Close()
}
