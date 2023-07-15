package transport

import (
	"github.com/yanmxa/straw/pkg/apis"
)

var _ Receiver = (*defaultReceiver)(nil)

type defaultReceiver struct {
	msgChan chan apis.TransportMessage
}

func NewDefaultReceiver(messageChan chan apis.TransportMessage) *defaultReceiver {
	return &defaultReceiver{
		msgChan: messageChan,
	}
}

func (r *defaultReceiver) Stop() {
	close(r.msgChan)
}

func (r *defaultReceiver) MessageChan() <-chan apis.TransportMessage {
	return r.msgChan
}
