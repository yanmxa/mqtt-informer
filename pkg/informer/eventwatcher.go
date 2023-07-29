package informer

import (
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/yanmxa/straw/pkg/apis"
	"github.com/yanmxa/straw/pkg/utils"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"
)

var _ EventWatcher = (*eventWatcher)(nil)

type EventWatcher interface {
	watch.Interface
	Add(event cloudevents.Event) error
}

type eventWatcher struct {
	uid             types.UID
	gvr             schema.GroupVersionResource
	stop            func(id string)
	watchResultChan chan watch.Event
}

func newEventWatcher(uid types.UID, gvr schema.GroupVersionResource, chanSize int, stop func(id string)) EventWatcher {
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
	w.stop(string(w.uid))
	close(w.watchResultChan)
}

func (w *eventWatcher) Add(event cloudevents.Event) error {
	if w.uid != types.UID(event.ID()) {
		return fmt.Errorf("unable to find the related event uid(%s) for watcher(%s) %s", event.ID(), w.uid)
	}

	watchResponse := &apis.WatchResponseEvent{}
	err := event.DataAs(watchResponse)
	if err != nil {
		return err
	}

	obj, err := convertToPartialObjectMetadata(watchResponse.Object)
	if err != nil {
		return err
	}

	// TODO
	utils.PrettyPrint(watchResponse)
	w.watchResultChan <- watch.Event{
		Type:   watchResponse.Type,
		Object: obj,
	}
	klog.Info("watcher add event: ", event.Type())
	return nil
}
