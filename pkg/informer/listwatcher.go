package informer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/yanmxa/straw/pkg/apis"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"

	"k8s.io/client-go/tools/cache"
)

type eventListWatcher struct {
	ctx            context.Context
	gvr            schema.GroupVersionResource
	namespace      string
	watcher        *messageWatcher
	listResultChan map[types.UID]chan apis.ListResponseMessage
	rwlock         sync.RWMutex

	transporter cloudevents.Client
}

func NewEventListWatcher(ctx context.Context, t cloudevents.Client, namespace string,
	gvr schema.GroupVersionResource,
) cache.ListerWatcher {
	lw := &eventListWatcher{
		ctx:            ctx,
		gvr:            gvr,
		namespace:      namespace,
		listResultChan: map[types.UID]chan apis.ListResponseMessage{},
		transporter:    t,
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

func (e *eventListWatcher) List(options metav1.ListOptions) (runtime.Object, error) {
	return e.list(e.ctx, options)
}

func (e *eventListWatcher) Watch(options metav1.ListOptions) (watch.Interface, error) {
	watchMessage := newListWatchMsg("informer", apis.MessageWatchType(e.gvr), e.namespace, e.gvr, options)
	transportMessage := watchMessage.ToMessage()

	e.transporter.Send(e.sendTopic, transportMessage)
	klog.Infof("request to watch message(%s) to %s", transportMessage.Type, e.sendTopic)

	e.watcher = newMessageWatcher(watchMessage.uid, e.watcherStop, e.gvr, 10)
	return e.watcher, nil
}

func (e *MessageListWatcher) watcherStop() {
	stopWatchMessage := newListWatchMsg("informer", apis.MessageStopWatchType(e.gvr), e.namespace, e.gvr,
		metav1.ListOptions{})
	transportMessage := stopWatchMessage.ToMessage()

	klog.Infof("request to stop watch message(%s): %s", transportMessage.Type, e.sendTopic)
	err := e.transporter.Send(e.sendTopic, transportMessage)
	if err != nil {
		klog.Error(err)
	}
}

func (e *MessageListWatcher) list(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	listMessageRequest := newListWatchMsg("informer", apis.MessageListType(e.gvr), e.namespace, e.gvr, options)
	transportMessage := listMessageRequest.ToMessage()

	klog.Infof("request to list message(%s) to %s", transportMessage.Type, e.sendTopic)
	err := e.transporter.Send(e.sendTopic, transportMessage)
	if err != nil {
		return nil, err
	}

	objectList := &unstructured.UnstructuredList{}
	// now start to receive the list response until endOfList is false
	e.listResultChan[listMessageRequest.uid] = make(chan apis.ListResponseMessage)
	defer delete(e.listResultChan, listMessageRequest.uid)
	listRunning := false
	for {
		select {
		case response, ok := <-e.listResultChan[listMessageRequest.uid]:
			if !ok {
				klog.Errorf("listResult chan(%s) is closed: %s", transportMessage.ID, transportMessage.Type)
				return objectList, nil
			}

			listRunning = true
			if objectList.Object == nil {
				objectList.Object = response.Objects.Object
			}

			objectList.Items = append(objectList.Items, response.Objects.Items...)
			if response.EndOfList {
				return objectList, nil
			}
		case <-ctx.Done():
			return objectList, nil
		case <-time.After(time.Second * 10):
			if listRunning {
				continue
			}
			klog.Infof("request to relist message(%s) to %s", transportMessage.Type, e.sendTopic)
			err := e.transporter.Send(e.sendTopic, transportMessage)
			if err != nil {
				return nil, err
			}
		}
	}
}
