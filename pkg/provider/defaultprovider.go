package provider

import (
	"context"
	"encoding/json"

	"github.com/yanmxa/transport-informer/pkg/apis"
	transport "github.com/yanmxa/transport-informer/pkg/transport"
	"github.com/yanmxa/transport-informer/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
)

type defaultProvider struct {
	clusterName string
	lw          ListWatcher // used to list and watch local resource
	transporter transport.Transport
	watchStop   map[types.UID]context.CancelFunc
}

func NewDefaultProvider(clusterName string, dynamicClient *dynamic.DynamicClient, t transport.Transport) Provider {
	return &defaultProvider{
		clusterName: clusterName,
		lw:          NewDynamicListWatcher(dynamicClient),
		transporter: t,
		watchStop:   map[types.UID]context.CancelFunc{},
	}
}

func (d *defaultProvider) Run(ctx context.Context) error {
	receiver, err := d.transporter.Receive()
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			klog.Info("context done!")
			return nil
		case transportMsg := <-receiver.MessageChan():
			err := d.process(ctx, transportMsg)
			if err != nil {
				klog.Error(err)
			}
		}
	}
}

func (d *defaultProvider) StopAll() {
	for _, stop := range d.watchStop {
		stop()
	}
}

func (d *defaultProvider) process(ctx context.Context, transportMsg apis.TransportMessage) error {
	mode, gvr, err := apis.ParseMessageType(transportMsg.Type)
	if err != nil {
		return err
	}
	req := &apis.RequestMessage{}
	err = json.Unmarshal(transportMsg.Payload, &req)
	if err != nil {
		return err
	}

	switch mode {
	case string(apis.ModeList):
		err := d.sendListResponses(ctx, types.UID(transportMsg.ID), req.Namespace, gvr, req.Options)
		if err != nil {
			klog.Error(err)
		}
	case string(apis.ModeWatch):
		go d.watchResponse(ctx, types.UID(transportMsg.ID), req.Namespace, gvr, req.Options)
	case string(apis.ModeStop):
		cancelFunc, ok := d.watchStop[types.UID(transportMsg.ID)]
		if ok {
			cancelFunc()
			delete(d.watchStop, types.UID(transportMsg.ID))
		}
	}
	return nil
}

func (d *defaultProvider) watchResponse(ctx context.Context, id types.UID, namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) {
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
				klog.Warning("failed to watch the result", "event", e)
				continue
			}

			obj, ok := e.Object.(*unstructured.Unstructured)
			if !ok {
				klog.Warning("failed to convert object to unstructured")
				continue
			}
			utils.ConvertToGlobalObj(obj, d.clusterName)

			response := &apis.WatchResponseMessage{
				Type:   e.Type,
				Object: obj,
			}
			res, err := json.Marshal(response)
			if err != nil {
				klog.Warning(err)
			}

			msg := apis.TransportMessage{}
			msg.ID = string(id)
			msg.Type = apis.MessageWatchResponseType(gvr)
			msg.Source = d.clusterName
			msg.Payload = res

			klog.Infof("send watch message(%s): %s", msg.ID, msg.Type)
			err = d.transporter.Send(msg)
			if err != nil {
				klog.Warning("failed to send watch object with error: %v", err)
			}
		case <-watchCtx.Done():
			return
		}
	}
}

func (d *defaultProvider) sendListResponses(ctx context.Context, id types.UID, namespace string,
	gvr schema.GroupVersionResource, options metav1.ListOptions,
) error {
	objs, err := d.lw.List(namespace, gvr, options)
	if err != nil {
		klog.Errorf("failed to list resource with err: %v", err)
		return err
	}

	for _, obj := range objs.Items {
		utils.ConvertToGlobalObj(&obj, d.clusterName)
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
	msg.Source = d.clusterName
	msg.Payload = res

	klog.Infof("send list response message(%s): %s", msg.ID, msg.Type)
	err = d.transporter.Send(msg)

	if err != nil {
		klog.Errorf("failed to send list objects with error: %v", err)
		return err
	}
	return nil
}
