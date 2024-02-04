package lib

type MemoryQueueFactor struct {
}

type MemoryEvent struct {
	payload []byte
}

func (e *MemoryEvent) GetPayload() []byte {
	return e.payload
}

func NewMemoryQueueFactory() (*MemoryQueueFactor, error) {
	return &MemoryQueueFactor{}, nil
}

type MemoryQueue struct {
}
