package sender

import (
	"context"
	"encoding/json"
	"fmt"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/yanmxa/transport-informer/pkg/apis"
	"github.com/yanmxa/transport-informer/pkg/config"
	"github.com/yanmxa/transport-informer/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
)

type defaultSender struct {
	lw              ListWatcher // used to list and watch local resource
	transportClient MQTT.Client
	signalChan      chan *apis.TransportMessage
	watchStop       map[types.UID]context.CancelFunc
}

func NewDefaultSender(dynamicClient *dynamic.DynamicClient, transportClient MQTT.Client) Sender {
	return &defaultSender{
		lw:              NewDynamicListWatcher(dynamicClient),
		transportClient: transportClient,
		signalChan:      make(chan *apis.TransportMessage),
		watchStop:       map[types.UID]context.CancelFunc{},
	}
}

func (d *defaultSender) Start(ctx context.Context, signalTopic string) {
	if !d.transportClient.IsConnected() {
		if token := d.transportClient.Connect(); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	}
	if token := d.transportClient.Subscribe(signalTopic, config.QoS, func(client MQTT.Client, msg MQTT.Message) {
		transportMsg := &apis.TransportMessage{}
		err := json.Unmarshal(msg.Payload(), transportMsg)
		if err != nil {
			klog.Error(err)
			return
		}
		klog.Infof("received signal: %s - %s", transportMsg.ID, transportMsg.Type)
		d.signalChan <- transportMsg
	}); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
}

func (d *defaultSender) Send(ctx context.Context, payloadTopic string) {
	for {
		select {
		case <-ctx.Done():
			klog.Info("context done")
			return
		case transportMsg := <-d.signalChan:
			err := d.process(ctx, transportMsg, payloadTopic)
			if err != nil {
				klog.Error(err)
			}
		}
	}
}

func (d *defaultSender) Stop() {
	for _, stop := range d.watchStop {
		stop()
	}
	d.transportClient.Disconnect(250)
}

func (d *defaultSender) process(ctx context.Context, transportMsg *apis.TransportMessage, payloadTopic string) error {
	mode, gvr, err := apis.ParseMessageType(transportMsg.Type)
	if err != nil {
		return err
	}
	req := &apis.RequestMessage{}
	err = json.Unmarshal(transportMsg.Payload, &req)
	if err != nil {
		return err
	}

	if !d.transportClient.IsConnected() {
		if token := d.transportClient.Connect(); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	}

	switch mode {
	case string(apis.ModeList):
		err := d.sendListResponses(ctx, types.UID(transportMsg.ID), req.Namespace, gvr, req.Options, payloadTopic)
		if err != nil {
			klog.Error(err)
		}
	case string(apis.ModeWatch):
		go d.watchResponse(ctx, types.UID(transportMsg.ID), req.Namespace, gvr, req.Options, payloadTopic)
	case string(apis.ModeStop):
		cancelFunc, ok := d.watchStop[types.UID(transportMsg.ID)]
		if ok {
			cancelFunc()
			delete(d.watchStop, types.UID(transportMsg.ID))
		}
	}
	return nil
}

func (d *defaultSender) watchResponse(ctx context.Context, id types.UID, namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions, sendTopic string) {
	w, err := d.lw.Watch(namespace, gvr, options)
	if err != nil {
		klog.Error(err)
	}

	watchCtx, stop := context.WithCancel(ctx)
	d.watchStop[id] = stop
	defer w.Stop()

	for {
		select {
		case e, ok := <-w.ResultChan():
			if !ok {
				klog.Error("failed to watch the result")
				return
			}

			obj := e.Object.(*unstructured.Unstructured)
			utils.ConvertToGlobalObj(obj, config.ClusterName)

			response := &apis.WatchResponseMessage{
				Type:   e.Type,
				Object: obj,
			}
			res, err := json.Marshal(response)
			if err != nil {
				klog.Error(err)
				return
			}

			msg := apis.TransportMessage{}
			msg.ID = string(id)
			msg.Type = apis.MessageWatchResponseType(gvr)
			msg.Source = "sender"
			msg.Payload = res

			payload, err := json.Marshal(msg)
			if err != nil {
				klog.Error(err)
				return
			}
			klog.Infof("send watch message(%s): %s", msg.ID, msg.Type)
			d.connect()
			token := d.transportClient.Publish(sendTopic, config.QoS, config.Retained, payload)
			token.Wait()
			if token.Error() != nil {
				klog.Error(token.Error())
			}
		case <-watchCtx.Done():
			return
		}
	}
}

func (d *defaultSender) connect() {
	if !d.transportClient.IsConnected() {
		if token := d.transportClient.Connect(); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	}
}

func (d *defaultSender) sendListResponses(ctx context.Context, id types.UID, namespace string,
	gvr schema.GroupVersionResource, options metav1.ListOptions, sendTopic string,
) error {
	objs, err := d.lw.List(namespace, gvr, options)
	if err != nil {
		klog.Errorf("failed to list resource with err: %v", err)
		return err
	}

	for _, obj := range objs.Items {
		utils.ConvertToGlobalObj(&obj, config.ClusterName)
	}

	response := &apis.ListResponseMessage{
		Objects:   objs,
		EndOfList: true,
	}
	res, err := json.Marshal(response)
	if err != nil {
		return err
	}
	// fmt.Println("send list response: ", response.Objects.Items[0].GetName())

	msg := apis.TransportMessage{}
	msg.ID = string(id)
	msg.Type = apis.MessageListResponseType(gvr)
	msg.Source = "sender"
	msg.Payload = res

	klog.Infof("send list response message(%s): %s", msg.ID, msg.Type)
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message %v", err)
	}

	token := d.transportClient.Publish(sendTopic, config.QoS, config.Retained, payload)
	token.Wait()
	if token.Error() != nil {
		klog.Errorf("failed to send request with error: %v", token.Error())
		return token.Error()
	}
	klog.Info("send list response successfully")
	return nil
}
