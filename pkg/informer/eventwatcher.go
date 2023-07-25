package informer

import (
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/yanmxa/straw/pkg/apis"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

var _ watch.Interface = (*eventWatcher)(nil)

type eventWatcher struct {
	uid             types.UID
	gvr             schema.GroupVersionResource
	stop            func()
	watchResultChan chan watch.Event
}

func newEventWatcher(uid types.UID, gvr schema.GroupVersionResource, chanSize int, stop func()) watch.Interface {
	return &eventWatcher{
		uid:             uid,
		gvr:             gvr,
		watchResultChan: make(chan watch.Event, chanSize),
		stop:            stop,
	}
}

func (w *eventWatcher) ResultChan() <-chan watch.Event {
	return w.watchResultChan
}

func (w *eventWatcher) Stop() {
	w.stop()
}

func (w *eventWatcher) process(event cloudevents.Event) error {
	if w.uid != types.UID(event.ID()) {
		return fmt.Errorf("unable to find the related event uid(%s) for watcher(%s) %s", event.ID(), w.uid)
	}

	watchResponse := &apis.WatchResponseEvent{}
	err := event.DataAs(watchResponse)
	if err != nil {
		return err
	}

	w.watchResultChan <- watch.Event{
		Type:   watchResponse.Type,
		Object: watchResponse.Object,
	}
	return nil
}
