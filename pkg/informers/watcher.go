package informers

import (
	"encoding/json"

	"github.com/yanmxa/mqtt-informer/pkg/apis"
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

func (w *messageWatcher) process(transportMsg TransportMessage) error {
	if w.uid != types.UID(transportMsg.ID) {
		return nil
	}

	if transportMsg.Type != apis.MessageWatchResponseType(w.gvr) {
		return nil
	}

	response := &apis.WatchResponseMessage{}

	err := json.Unmarshal(transportMsg.Payload, response)
	if err != nil {
		return err
	}
	// response, ok := message.Payload.(apis.WatchResponseMessage)
	// if !ok {
	// 	return fmt.Errorf("failed message type")
	// }

	watchEvent := &watch.Event{
		Type:   response.Type,
		Object: response.Object,
	}

	w.result <- *watchEvent
	return nil
}
