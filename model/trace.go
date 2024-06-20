package model

import (
	"time"
)

type Trace struct {
	ENTER     string
	EXIT      string
	_app      string
	_event    string
	_timestamp float64
}

func NewTrace(app, event string, timestamp float64) *Trace {
	if timestamp == 0 {
		timestamp = float64(time.Now().UnixNano()) / float64(time.Second)
	}
	return &Trace{
		ENTER:     "enter",
		EXIT:      "exit",
		_app:      app,
		_event:    event,
		_timestamp: timestamp,
	}
}

func (t *Trace) GetApp() string {
	return t._app
}

func (t *Trace) GetEvent() string {
	return t._event
}

func (t *Trace) GetTimestamp() float64 {
	return t._timestamp
}

func (t *Trace) ToDict() map[string]interface{} {
	return map[string]interface{}{
		"app":       t._app,
		"event":     t._event,
		"timestamp": t._timestamp,
	}
}
