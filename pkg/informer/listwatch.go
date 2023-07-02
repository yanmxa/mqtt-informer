package informer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/yanmxa/transport-informer/pkg/apis"
	"github.com/yanmxa/transport-informer/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"
)

type MessageListWatcher struct {
	ctx            context.Context
	gvr            schema.GroupVersionResource
	namespace      string
	watcher        *messageWatcher
	listResultChan map[types.UID]chan apis.ListResponseMessage
	rwlock         sync.RWMutex

	transporter             transport.Transport
	sendTopic, receiveTopic string
}

func NewMessageListWatcher(ctx context.Context, t transport.Transport, namespace string,
	gvr schema.GroupVersionResource, send, receive string,
) *MessageListWatcher {
	lw := &MessageListWatcher{
		ctx:            ctx,
		gvr:            gvr,
		namespace:      namespace,
		listResultChan: map[types.UID]chan apis.ListResponseMessage{},
		transporter:    t,
		sendTopic:      send,
		receiveTopic:   receive,
	}

	receiver, err := t.Receive(receive)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				klog.Info("context done! stop receive message...")
				return
			case transportMsg := <-receiver.MessageChan():
				err := lw.process(ctx, &transportMsg)
				if err != nil {
					klog.Error(err)
				}
			}
		}
	}()

	return lw
}

func (lw *MessageListWatcher) process(ctx context.Context, transportMessage *apis.TransportMessage) error {
	lw.rwlock.RLock()
	defer lw.rwlock.RUnlock()
	// klog.Infof("received message(%s): %s", transportMessage.ID, transportMessage.Type)

	switch transportMessage.Type {
	case apis.MessageListResponseType(lw.gvr): // response.list.%s
		resultChan, ok := lw.listResultChan[types.UID(transportMessage.ID)]
		if !ok {
			return fmt.Errorf("unable to find the related uid for list %s", transportMessage.ID)
		}

		listResponse := &apis.ListResponseMessage{}
		err := json.Unmarshal(transportMessage.Payload, listResponse)
		if err != nil {
			return err
		}
		resultChan <- *listResponse
	case apis.MessageWatchResponseType(lw.gvr):
		if lw.watcher == nil {
			return fmt.Errorf("unable to find the related uid for watch %s", transportMessage.ID)
		}
		err := lw.watcher.process(*transportMessage)
		if err != nil {
			return fmt.Errorf("unable to process message %s", transportMessage.Type)
		}
	}
	return nil
}

func (e *MessageListWatcher) List(options metav1.ListOptions) (runtime.Object, error) {
	return e.list(e.ctx, options)
}

func (e *MessageListWatcher) Watch(options metav1.ListOptions) (watch.Interface, error) {
	watchMessage := newListWatchMsg("informer", apis.MessageWatchType(e.gvr), e.namespace, e.gvr, options)
	transportMessage := watchMessage.ToMessage()

	e.transporter.Send(e.sendTopic, transportMessage)
	klog.Infof("request to watch message(%s): %s", transportMessage.ID, transportMessage.Type)

	e.watcher = newMessageWatcher(watchMessage.uid, e.watcherStop, e.gvr, 10)
	return e.watcher, nil
}

func (e *MessageListWatcher) watcherStop() {
	stopWatchMessage := newListWatchMsg("informer", apis.MessageStopWatchType(e.gvr), e.namespace, e.gvr,
		metav1.ListOptions{})
	transportMessage := stopWatchMessage.ToMessage()

	klog.Infof("request to stop watch message(%s): %s", transportMessage.ID, transportMessage.Type)
	err := e.transporter.Send(e.sendTopic, transportMessage)
	if err != nil {
		klog.Error(err)
	}
}

func (e *MessageListWatcher) list(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	listMessageRequest := newListWatchMsg("informer", apis.MessageListType(e.gvr), e.namespace, e.gvr, options)
	transportMessage := listMessageRequest.ToMessage()

	klog.Infof("request to list message(%s): %s", transportMessage.ID, transportMessage.Type)
	err := e.transporter.Send(e.sendTopic, transportMessage)
	if err != nil {
		return nil, err
	}

	objectList := &unstructured.UnstructuredList{}
	// now start to receive the list response until endOfList is false
	e.listResultChan[listMessageRequest.uid] = make(chan apis.ListResponseMessage)
	defer delete(e.listResultChan, listMessageRequest.uid)
	for {
		select {
		case response, ok := <-e.listResultChan[listMessageRequest.uid]:
			if !ok {
				return objectList, nil
			}

			if objectList.Object == nil {
				objectList.Object = response.Objects.Object
			}

			objectList.Items = append(objectList.Items, response.Objects.Items...)
			if response.EndOfList {
				return objectList, nil
			}
		case <-ctx.Done():
			return objectList, nil
		}
	}
}

type ListWatchRequest interface {
	ToMessage() apis.TransportMessage
}

type ListWatchRequestMsg struct {
	uid       types.UID
	gvr       schema.GroupVersionResource
	options   metav1.ListOptions
	mode      string
	source    string
	namespace string
}

func newListWatchMsg(source, mode, namespace string, gvr schema.GroupVersionResource,
	options metav1.ListOptions,
) *ListWatchRequestMsg {
	return &ListWatchRequestMsg{
		uid:       types.UID(uuid.New().String()),
		gvr:       gvr,
		options:   options,
		mode:      mode,
		namespace: namespace,
		source:    source,
	}
}

func (l *ListWatchRequestMsg) ToMessage() apis.TransportMessage {
	msg := apis.TransportMessage{}

	data := &apis.RequestMessage{
		Namespace: l.namespace,
		Options:   l.options,
	}

	msg.Type = l.mode
	msg.ID = string(l.uid)
	msg.Payload, _ = json.Marshal(data)
	msg.Source = l.source
	return msg
}
