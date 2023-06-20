package senders

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/yanmxa/mqtt-informer/pkg/apis"
	"github.com/yanmxa/mqtt-informer/pkg/config"
	"github.com/yanmxa/mqtt-informer/pkg/informers"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

type defaultSenderTransport struct {
	sender    Sender // used to list and watch local resource
	sclient   MQTT.Client
	rclient   MQTT.Client
	watchStop map[types.UID]context.CancelFunc
}

func NewDefaultSenderTransport(sender Sender, sclient, rclient MQTT.Client) SenderTransport {
	return &defaultSenderTransport{
		sender:    sender,
		sclient:   sclient,
		rclient:   rclient,
		watchStop: map[types.UID]context.CancelFunc{},
	}
}

func (d *defaultSenderTransport) Run(ctx context.Context) {
	if token := d.rclient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	// fmt.Println("Subscriber Starting")
	if token := d.rclient.Subscribe(config.ReceiveTopic, config.QoS, func(client MQTT.Client, msg MQTT.Message) {
		fmt.Println("received message: ", msg.Topic(), string(msg.Payload()))

		transportMsg := &informers.TransportMessage{}
		err := json.Unmarshal(msg.Payload(), transportMsg)
		if err != nil {
			klog.Error(err)
			return
		}
		mode, gvr, err := apis.ParseMessageType(transportMsg.Type)
		if err != nil {
			klog.Error(err)
			return
		}

		req := &apis.RequestMessage{}
		err = json.Unmarshal(transportMsg.Payload, &req)
		if err != nil {
			klog.Error(err)
			return
		}

		klog.Infof("received request of %v", req)

		switch mode {
		case "list":
			err := d.sendListResponses(ctx, types.UID(transportMsg.ID), req.Namespace, gvr, req.Options)
			if err != nil {
				klog.Error(err)
			}
		case "watch":
			go d.watchResponse(ctx, types.UID(transportMsg.ID), req.Namespace, gvr, req.Options)
		case "stopwatch":
			cancelFunc, ok := d.watchStop[types.UID(transportMsg.ID)]
			if ok {
				cancelFunc()
				delete(d.watchStop, types.UID(transportMsg.ID))
			}
		}
	}); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	<-ctx.Done()
	d.rclient.Disconnect(250)
	fmt.Println("Subscriber Disconnected")
}

func (d *defaultSenderTransport) watchResponse(ctx context.Context, id types.UID, namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) error {
	w, err := d.sender.Watch(namespace, gvr, options)
	if err != nil {
		return err
	}

	watchCtx, stop := context.WithCancel(ctx)
	d.watchStop[id] = stop
	defer w.Stop()

	if token := d.sclient.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	defer d.sclient.Disconnect(250)

	for {
		select {
		case e, ok := <-w.ResultChan():
			if !ok {
				return fmt.Errorf("failed to watch the result")
			}

			response := &apis.WatchResponseMessage{
				Type:   e.Type,
				Object: e.Object.(*unstructured.Unstructured),
			}
			payload, err := json.Marshal(response)
			if err != nil {
				return err
			}

			msg := informers.TransportMessage{}
			msg.ID = string(id)
			msg.Type = apis.MessageWatchResponseType(gvr)
			msg.Source = "server"
			msg.Payload = payload

			klog.Infof("send watch response for resource %v", gvr)
			token := d.sclient.Publish(config.SendTopic, config.QoS, config.Retained, msg)
			token.Wait()
			if token.Error() != nil {
				klog.Error(token.Error())
			}
		case <-watchCtx.Done():
			return nil
		}
	}
}

func (d *defaultSenderTransport) sendListResponses(ctx context.Context, id types.UID, namespace string,
	gvr schema.GroupVersionResource, options metav1.ListOptions,
) error {
	objs, err := d.sender.List(namespace, gvr, options)
	if err != nil {
		klog.Errorf("failed to list resource with err: %v", err)
		return err
	}

	if token := d.sclient.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	defer d.sclient.Disconnect(100)

	response := &apis.ListResponseMessage{
		Objects:   objs,
		EndOfList: true,
	}
	payload, err := json.Marshal(response)
	if err != nil {
		return err
	}

	msg := informers.TransportMessage{}
	msg.ID = string(id)
	msg.Type = apis.MessageWatchResponseType(gvr)
	msg.Source = "server"
	msg.Payload = payload

	klog.Infof("send list response for resource %v", gvr)
	token := d.sclient.Publish(config.SendTopic, config.QoS, config.Retained, msg)
	token.Wait()
	if token.Error() != nil {
		klog.Errorf("failed to send request with error: %v", err)
		return token.Error()
	}
	return nil
}
