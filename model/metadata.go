package model

import "fmt"

type Metadata struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewMetadata(key, value string) *Metadata {
	return &Metadata{
		Key:   key,
		Value: value,
	}
}

func (m *Metadata) GetKey() string {
	return m.Key
}

func (m *Metadata) GetValue() string {
	return m.Value
}

func (m *Metadata) ToDict() map[string]string {
	return map[string]string{
		"key":   m.Key,
		"value": m.Value,
	}
}