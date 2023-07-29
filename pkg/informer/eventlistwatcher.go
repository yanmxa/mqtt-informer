package informer

import (
	"context"
	"fmt"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/yanmxa/straw/pkg/apis"
	"github.com/yanmxa/straw/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"

	"k8s.io/client-go/tools/cache"
)

var _ cache.ListerWatcher = (*eventListWatcher)(nil)

type eventListWatcher struct {
	ctx            context.Context
	gvr            schema.GroupVersionResource
	namespace      string
	source         string
	watcher        EventWatcher
	listResultChan map[types.UID]chan apis.ListResponseEvent
	rwlock         sync.RWMutex

	transporter cloudevents.Client
}

func NewEventListWatcher(ctx context.Context, t cloudevents.Client, namespace string,
	gvr schema.GroupVersionResource, source string,
) cache.ListerWatcher {
	lw := &eventListWatcher{
		ctx:            ctx,
		gvr:            gvr,
		namespace:      namespace,
		source:         source,
		listResultChan: map[types.UID]chan apis.ListResponseEvent{},
		transporter:    t,
	}

	go t.StartReceiver(ctx, func(event cloudevents.Event) error {
		lw.rwlock.RLock()
		defer lw.rwlock.RUnlock()
		klog.Infof("received response event %s", event.Type())
		switch event.Type() {
		case apis.EventListResponseType(gvr):
			resultChan, ok := lw.listResultChan[types.UID(event.ID())]
			if !ok {
				return fmt.Errorf("unable to find the related uid for list %s", event.ID())
			}
			response := &apis.ListResponseEvent{}
			err := event.DataAs(response)
			if err != nil {
				return err
			}
			resultChan <- *response
		case apis.EventWatchResponseType(gvr):
			if lw.watcher == nil {
				return fmt.Errorf("unable to find the watcher for gvr %s", gvr)
			}
			return lw.watcher.Add(event)
		}
		return nil
	})

	return lw
}

func (e *eventListWatcher) List(options metav1.ListOptions) (runtime.Object, error) {
	sessionId := uuid.New().String()
	listRequestEvent, err := e.toEvent(sessionId, apis.EventListType(e.gvr), options)
	if err != nil {
		return nil, err
	}
	result := e.transporter.Send(e.ctx, listRequestEvent)
	if cloudevents.IsUndelivered(result) {
		return nil, fmt.Errorf("failed to send list event, %v", result)
	}
	klog.Infof("request to list event: %s", listRequestEvent.Type())
	e.listResultChan[types.UID(sessionId)] = make(chan apis.ListResponseEvent)
	defer delete(e.listResultChan, types.UID(sessionId))

	objectList := &unstructured.UnstructuredList{}
	// TODO
	defer func() {
		utils.PrettyPrint(objectList)
	}()
	for {
		select {
		case response, ok := <-e.listResultChan[types.UID(sessionId)]:
			if !ok {
				klog.Errorf("listResult chan(%s) is closed: %s", sessionId, listRequestEvent.Type())
				return objectList, nil
			}

			if objectList.Object == nil {
				objectList.Object = response.Objects.Object
			}

			objectList.Items = append(objectList.Items, response.Objects.Items...)
			if response.EndOfList {
				return objectList, nil
			}
		case <-e.ctx.Done():
			return objectList, nil
		}
	}
}

func (e *eventListWatcher) Watch(options metav1.ListOptions) (watch.Interface, error) {
	sessionId := uuid.New().String()
	watchRequestEvent, err := e.toEvent(sessionId, apis.EventWatchType(e.gvr), options)
	if err != nil {
		return nil, err
	}
	result := e.transporter.Send(e.ctx, watchRequestEvent)
	if cloudevents.IsUndelivered(result) {
		return nil, fmt.Errorf("failed to send watch event: %v", result)
	}
	klog.Infof("request to watch: %s", watchRequestEvent.Type())
	e.watcher = newEventWatcher(types.UID(sessionId), e.gvr, 10, e.watcherStop)
	return e.watcher, nil
}

func (e *eventListWatcher) watcherStop(watcherId string) {
	stopWatchRequestEvent, err := e.toEvent(watcherId, apis.EventStopWatchType(e.gvr), metav1.ListOptions{})
	if err != nil {
		klog.Error(err)
		return
	}
	klog.Infof("request to stopwatch: %s - %s \n", stopWatchRequestEvent.Type(), stopWatchRequestEvent.ID())
	// since the context is canceled, we should send the message with a new context
	result := e.transporter.Send(context.TODO(), stopWatchRequestEvent)
	if cloudevents.IsUndelivered(result) {
		klog.Error("failed to send watch event: %v", result)
	}
}

func (e *eventListWatcher) toEvent(id string, eventType string, options metav1.ListOptions) (cloudevents.Event, error) {
	event := cloudevents.NewEvent()
	event.SetID(id)
	event.SetType(eventType)
	event.SetSource(e.source)
	data := &apis.RequestEvent{
		Namespace: e.namespace,
		Options:   options,
	}
	err := event.SetData(cloudevents.ApplicationJSON, data)
	return event, err
}
