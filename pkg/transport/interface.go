package transport

import "github.com/yanmxa/transport-informer/pkg/apis"

type Transport interface {
	Sender
	Receive() (Receiver, error)
	Stop()
}

type Sender interface {
	Send(msg apis.TransportMessage) error
}

type Receiver interface {
	// Stop stops watching. Will close the channel returned by ResultChan(). Releases
	// any resources used by the watch.
	Stop()

	// MessageChan returns a chan which will receive all the transportMessage. If an error occurs
	// or Stop() is called, the implementation will close this channel and
	// release any resources used by the watch.
	MessageChan() <-chan apis.TransportMessage
}
