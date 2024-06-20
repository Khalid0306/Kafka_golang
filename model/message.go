package model

type Message struct {
	Payload   interface{}  `json:"payload"`
	Traces    []*Trace     `json:"traces"`
	Metadatas []*Metadata  `json:"metadatas"`
}

func NewMessage(payload interface{}, metadatas []*Metadata, traces []*Trace) *Message {
	return &Message{
		Payload:   payload,
		Traces:    traces,
		Metadatas: metadatas,
	}
}

func (m *Message) GetPayload() interface{} {
	return m.Payload
}

func (m *Message) SetPayload(payload interface{}) {
	m.Payload = payload
}

func (m *Message) GetTraces() []*Trace {
	return m.Traces
}

func (m *Message) SetTraces(traces []*Trace) {
	m.Traces = traces
}

func (m *Message) AddTrace(trace *Trace) {
	m.Traces = append(m.Traces, trace)
}

func (m *Message) GetMetadatas() []*Metadata {
	return m.Metadatas
}

func (m *Message) GetMetadatasByKey(key string) []*Metadata {
	var result []*Metadata
	for _, metadata := range m.Metadatas {
		if metadata.GetKey() == key {
			result = append(result, metadata)
		}
	}
	return result
}

func (m *Message) SetMetadatas(metadatas []*Metadata) {
	m.Metadatas = metadatas
}

func (m *Message) AddMetadata(metadata *Metadata) {
	m.Metadatas = append(m.Metadatas, metadata)
}

func (m *Message) ToDict() map[string]interface{} {
	tracesDict := make([]map[string]interface{}, len(m.Traces))
	for i, trace := range m.Traces {
		tracesDict[i] = trace.ToDict()
	}

	metadatasDict := make([]map[string]string, len(m.Metadatas))
	for i, metadata := range m.Metadatas {
		metadatasDict[i] = metadata.ToDict()
	}

	return map[string]interface{}{
		"payload":   m.Payload,
		"traces":    tracesDict,
		"metadatas": metadatasDict,
	}
}

