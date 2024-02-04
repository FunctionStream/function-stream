package lib

type SourceEvent interface {
	GetPayload() []byte
}

type SinkEvent interface {
	GetPayload() []byte
	GetSourceEvent() SourceEvent
}

type SinkEventImpl struct {
	payload     []byte
	sourceEvent SourceEvent
}

func NewSinkEventImpl(payload []byte, sourceEvent SourceEvent) SinkEvent {
	return &SinkEventImpl{
		payload:     payload,
		sourceEvent: sourceEvent,
	}
}

func (s *SinkEventImpl) GetPayload() []byte {
	return s.payload
}

func (s *SinkEventImpl) GetSourceEvent() SourceEvent {
	return s.sourceEvent
}
