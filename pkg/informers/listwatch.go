package informers

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/yanmxa/mqtt-informer/pkg/apis"
	"github.com/yanmxa/mqtt-informer/pkg/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"
)

type MessageListWatcher struct {
	client         MQTT.Client
	gvr            schema.GroupVersionResource
	source         string
	namespace      string
	ctx            context.Context
	watcher        *messageWatcher
	listResultChan map[types.UID]chan apis.ListResponseMessage
	rwlock         sync.RWMutex
}

func NewMessageListWatcher(ctx context.Context, source, namespace string, client MQTT.Client,
	gvr schema.GroupVersionResource,
) *MessageListWatcher {
	lw := &MessageListWatcher{
		source:         source,
		client:         client,
		gvr:            gvr,
		ctx:            ctx,
		namespace:      namespace,
		listResultChan: map[types.UID]chan apis.ListResponseMessage{},
	}

	// get message from subscribed topic
	go func() {
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			klog.Error(token.Error())
			return
		}

		// start list/watch receiver
		fmt.Println("Subscriber Starting")
		if token := client.Subscribe(config.ReceiveTopic, config.QoS, func(client MQTT.Client, msg MQTT.Message) {
			lw.rwlock.RLock()
			defer lw.rwlock.RUnlock()

			transportMessage := &TransportMessage{}
			err := json.Unmarshal(msg.Payload(), transportMessage)
			if err != nil {
				klog.Error(err)
				return
			}
			if transportMessage.Source == "agent" {
				klog.Infof("this message(%s - %s) is from agent, skip it", transportMessage.ID, transportMessage.Type)
				return
			}
			klog.Infof("received manager message(%s): %s", transportMessage.ID, transportMessage.Type)

			switch transportMessage.Type {
			case apis.MessageListResponseType(lw.gvr): // response.list.%s
				resultChan, ok := lw.listResultChan[types.UID(transportMessage.ID)]
				if !ok {
					klog.Error(fmt.Errorf("unable to find the related uid for list %s", transportMessage.ID))
				}

				listResponse := &apis.ListResponseMessage{}
				err := json.Unmarshal(transportMessage.Payload, listResponse)
				if err != nil {
					klog.Error(err)
					return
				}

				resultChan <- *listResponse
			case apis.MessageWatchResponseType(lw.gvr):
				if lw.watcher == nil {
					return
				}
				err := lw.watcher.process(*transportMessage)
				if err != nil {
					klog.Error(fmt.Errorf("unable to process message %s", transportMessage.Type))
				}
			}
		}); token.Wait() && token.Error() != nil {
			klog.Error(token.Error())
		}

		<-ctx.Done()
		client.Disconnect(250)
		fmt.Println("Subscriber Disconnected")
	}()

	return lw
}

func (e *MessageListWatcher) List(options metav1.ListOptions) (runtime.Object, error) {
	return e.list(e.ctx, options)
}

func (e *MessageListWatcher) Watch(options metav1.ListOptions) (watch.Interface, error) {
	return e.watch(e.ctx, options)
}

func (e *MessageListWatcher) watch(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	watchMessage := newListWatchMsg(e.source, apis.MessageWatchType(e.gvr), e.namespace, e.gvr, options)
	transportMessage := watchMessage.ToMessage()

	payload, err := json.Marshal(transportMessage)
	if err != nil {
		return nil, err
	}

	// send the watch message, TODO: change it to sender topic
	if !e.client.IsConnected() {
		if token := e.client.Connect(); token.Wait() && token.Error() != nil {
			return nil, token.Error()
		}
	}
	token := e.client.Publish(config.SendTopic, config.QoS, config.Retained, payload)
	token.Wait()
	if token.Error() != nil {
		return nil, token.Error()
	}

	klog.Infof("request to watch message(%s): %s", transportMessage.ID, transportMessage.Type)
	e.watcher = newMessageWatcher(watchMessage.uid, e.stopWatch, e.gvr, 10)

	return e.watcher, nil
}

func (e *MessageListWatcher) stopWatch() {
	stopWatchMessage := newListWatchMsg(e.source, apis.MessageStopWatchType(e.gvr), e.namespace, e.gvr,
		metav1.ListOptions{})
	transportMessage := stopWatchMessage.ToMessage()

	// send the watch message, TODO: change it to sender topic
	if token := e.client.Connect(); token.Wait() && token.Error() != nil {
		utilruntime.HandleError(token.Error())
	}
	token := e.client.Publish(config.SendTopic, config.QoS, config.Retained, transportMessage)
	token.Wait()
	if token.Error() != nil {
		utilruntime.HandleError(token.Error())
	}
	defer e.client.Disconnect(10)
}

func (e *MessageListWatcher) list(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	listMessage := newListWatchMsg(e.source, apis.MessageListType(e.gvr), e.namespace, e.gvr, options)
	transportMessage := listMessage.ToMessage()

	payload, err := json.Marshal(transportMessage)
	if err != nil {
		return nil, err
	}

	// send the list message, TODO: change it to sender topic
	if !e.client.IsConnected() {
		if token := e.client.Connect(); token.Wait() && token.Error() != nil {
			return nil, token.Error()
		}
	}

	token := e.client.Publish(config.SendTopic, config.QoS, config.Retained, payload)
	token.Wait()
	if token.Error() != nil {
		return nil, token.Error()
	}

	klog.Infof("request to list message(%s): %s", transportMessage.ID, transportMessage.Type)

	objectList := &unstructured.UnstructuredList{}

	// now start to receive the list response until endOfList is false
	e.listResultChan[listMessage.uid] = make(chan apis.ListResponseMessage)
	defer delete(e.listResultChan, listMessage.uid)
	for {
		select {
		case response, ok := <-e.listResultChan[listMessage.uid]:
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

type TransportMessage struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Source  string `json:"source"`
	Payload []byte `json:"payload"`
}

type ListWatchMessage interface {
	ToMessage() TransportMessage
}

type ListWatchMsg struct {
	uid       types.UID
	gvr       schema.GroupVersionResource
	options   metav1.ListOptions
	mode      string
	source    string
	namespace string
}

func newListWatchMsg(source, mode, namespace string, gvr schema.GroupVersionResource,
	options metav1.ListOptions,
) *ListWatchMsg {
	return &ListWatchMsg{
		uid:       types.UID(uuid.New().String()),
		gvr:       gvr,
		options:   options,
		mode:      mode,
		namespace: namespace,
		source:    source,
	}
}

func (l *ListWatchMsg) ToMessage() TransportMessage {
	msg := TransportMessage{}

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
