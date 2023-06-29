package informer

import (
	"encoding/json"

	"github.com/yanmxa/transport-informer/pkg/apis"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

type messageWatcher struct {
	uid    types.UID
	gvr    schema.GroupVersionResource
	stop   func()
	result chan watch.Event
}

func newMessageWatcher(uid types.UID, stop func(), gvr schema.GroupVersionResource, chanSize int) *messageWatcher {
	return &messageWatcher{
		uid:    uid,
		gvr:    gvr,
		result: make(chan watch.Event, chanSize),
		stop:   stop,
	}
}

func (w *messageWatcher) ResultChan() <-chan watch.Event {
	return w.result
}

func (w *messageWatcher) Stop() {
	w.stop()
}

func (w *messageWatcher) process(transportMsg apis.TransportMessage) error {
	// klog.Infof("process message(%s): %s", transportMsg.ID, transportMsg.Type)
	if w.uid != types.UID(transportMsg.ID) {
		return nil
	}

	if transportMsg.Type != apis.MessageWatchResponseType(w.gvr) {
		return nil
	}

	watchResponse := &apis.WatchResponseMessage{}

	err := json.Unmarshal(transportMsg.Payload, watchResponse)
	if err != nil {
		return err
	}
	// watchRes, err := json.Marshal(watchResponse)
	// if err != nil {
	// 	return err
	// }
	// fmt.Println(string(watchRes))
	partialObj, err := convertToPartialObjectMetadata(watchResponse.Object)
	if err != nil {
		return err
	}

	watchEvent := &watch.Event{
		Type:   watchResponse.Type,
		Object: partialObj,
	}
	// klog.Infof("send watch event(%s/%s): %s", partialObj.Namespace, partialObj.Name, watchEvent.Type)
	w.result <- *watchEvent
	return nil
}

func convertToPartialObjectMetadata(obj *unstructured.Unstructured) (*v1.PartialObjectMetadata, error) {
	partialObj := &v1.PartialObjectMetadata{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, partialObj)
	if err != nil {
		return nil, err
	}
	return partialObj, nil
}
