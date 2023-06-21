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
	client    MQTT.Client
	watchStop map[types.UID]context.CancelFunc
}

func NewDefaultSenderTransport(sender Sender, client MQTT.Client) SenderTransport {
	return &defaultSenderTransport{
		sender:    sender,
		client:    client,
		watchStop: map[types.UID]context.CancelFunc{},
	}
}

func (d *defaultSenderTransport) Run(ctx context.Context) {
	if !d.client.IsConnected() {
		if token := d.client.Connect(); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	}

	fmt.Println("Subscriber Starting")
	if token := d.client.Subscribe(config.ReceiveTopic, config.QoS, func(client MQTT.Client, msg MQTT.Message) {
		transportMsg := &informers.TransportMessage{}
		err := json.Unmarshal(msg.Payload(), transportMsg)
		if err != nil {
			klog.Error(err)
			return
		}

		fmt.Println("received message: ", transportMsg.ID, transportMsg.Type)

		// hack for one topic
		if transportMsg.Source == "manager" {
			klog.Infof("this message is from manager(%s - %s), skip it", transportMsg.ID, transportMsg.Type)
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

	fmt.Println("Subscriber Started")
	<-ctx.Done()
	// d.client.Disconnect(250)
	// fmt.Println("Subscriber Disconnected")
}

func (d *defaultSenderTransport) watchResponse(ctx context.Context, id types.UID, namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) error {
	w, err := d.sender.Watch(namespace, gvr, options)
	if err != nil {
		return err
	}

	watchCtx, stop := context.WithCancel(ctx)
	d.watchStop[id] = stop
	defer w.Stop()

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
			res, err := json.Marshal(response)
			if err != nil {
				return err
			}

			msg := informers.TransportMessage{}
			msg.ID = string(id)
			msg.Type = apis.MessageWatchResponseType(gvr)
			msg.Source = "manager"
			msg.Payload = res

			payload, err := json.Marshal(msg)
			if err != nil {
				return fmt.Errorf("failed to marshal message %v", err)
			}
			klog.Infof("send watch message(%s): %s", msg.ID, msg.Type)
			token := d.client.Publish(config.SendTopic, config.QoS, config.Retained, payload)
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

	response := &apis.ListResponseMessage{
		Objects:   objs,
		EndOfList: true,
	}
	res, err := json.Marshal(response)
	if err != nil {
		return err
	}
	// fmt.Println("send list response: ", response.Objects.Items[0].GetName())

	msg := informers.TransportMessage{}
	msg.ID = string(id)
	msg.Type = apis.MessageListResponseType(gvr)
	msg.Source = "manager"
	msg.Payload = res

	klog.Infof("send list response message(%s): %s", msg.ID, msg.Type)
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message %v", err)
	}

	token := d.client.Publish(config.SendTopic, config.QoS, config.Retained, payload)
	token.Wait()
	if token.Error() != nil {
		klog.Errorf("failed to send request with error: %v", token.Error())
		return token.Error()
	}
	klog.Info("send list response successfully")
	return nil
}
