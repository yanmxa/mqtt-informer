package transport

import (
	"github.com/yanmxa/transport-informer/pkg/apis"
)

var _ Receiver = (*defaultReceiver)(nil)

type defaultReceiver struct {
	msgChan chan apis.TransportMessage
}

func NewDefaultReceiver() *defaultReceiver {
	return &defaultReceiver{
		msgChan: make(chan apis.TransportMessage),
	}
}

func (r *defaultReceiver) Stop() {
	close(r.msgChan)
}

func (r *defaultReceiver) MessageChan() <-chan apis.TransportMessage {
	return r.msgChan
}

func (r *defaultReceiver) Forward(msg apis.TransportMessage) {
	r.msgChan <- msg
}
