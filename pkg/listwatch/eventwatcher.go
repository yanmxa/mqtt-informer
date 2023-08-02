package listwatch

import (
	"fmt"
	"reflect"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/yanmxa/straw/pkg/apis"
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

	// obj := GetObject(apis.ToGVRString(w.gvr))
	// if obj == nil {
	// 	return fmt.Errorf("the informer is unable to find the runtime object for gvr(%s)", apis.ToGVRString(w.gvr))
	// }

	// err = runtime.DefaultUnstructuredConverter.FromUnstructured(watchResponse.Object.Object, obj)
	// if err != nil {
	// 	return err
	// }
	t := reflect.TypeOf(watchResponse.Object)
	fmt.Println("received unstructured event type", t)

	// utils.PrettyPrint(obj)
	w.watchResultChan <- watch.Event{
		Type:   watchResponse.Type,
		Object: watchResponse.Object,
	}
	klog.Info("watcher add event: ", event.Type())
	return nil
}

// func convertUnstructuredObjToPartialObjectMetadata(obj *unstructured.Unstructured) (*v1.PartialObjectMetadata, error) {
// 	partialObj := &v1.PartialObjectMetadata{}
// 	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, partialObj)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return partialObj, nil
// }
