package provider

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/yanmxa/straw/pkg/apis"
	"github.com/yanmxa/straw/pkg/utils"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
)

type genericProvider struct {
	clusterName   string
	watcherStop   map[types.UID]context.CancelFunc
	tweakFunc     func(obj metav1.Object, clusterName string)
	transporter   cloudevents.Client
	dynamicClient *dynamic.DynamicClient
}

func NewProvider(clusterName string, dynamicClient *dynamic.DynamicClient, t cloudevents.Client,
	tweakFunc func(obj metav1.Object, clusterName string),
) Provider {
	return &genericProvider{
		clusterName:   clusterName,
		dynamicClient: dynamicClient,
		transporter:   t,
		watcherStop:   map[types.UID]context.CancelFunc{},
		tweakFunc:     tweakFunc,
	}
}

func (p *genericProvider) Run(ctx context.Context) error {
	return p.transporter.StartReceiver(ctx, func(evt cloudevents.Event) error {
		mode, gvr, err := apis.ParseEventType(evt.Type())
		if err != nil {
			return err
		}
		reqEvent := &apis.RequestEvent{}

		err = evt.DataAs(reqEvent)
		if err != nil {
			return err
		}
		klog.Info("provider receive event: ", evt.Type())

		switch mode {
		case string(apis.ModeList):
			err := p.sendListResponses(ctx, types.UID(evt.ID()), reqEvent.Namespace, gvr, reqEvent.Options)
			if err != nil {
				klog.Errorf("failed to send list response with error: %v", err)
			}
		case string(apis.ModeWatch):
			go p.watchResponse(ctx, types.UID(evt.ID()), reqEvent.Namespace, gvr, reqEvent.Options)
		case string(apis.ModeStop):
			cancelFunc, ok := p.watcherStop[types.UID(evt.ID())]
			if ok {
				cancelFunc()
				delete(p.watcherStop, types.UID(evt.ID()))
				klog.Info("provider stop watcher: ", evt.Type(), " - ", evt.ID())
			}
		default:
			klog.Warningf("unknown message type: %s", evt.Type())
		}
		return nil
	})
}

func (p *genericProvider) watchResponse(ctx context.Context, id types.UID, namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) {
	watchCtx, cancel := context.WithCancel(ctx)
	p.watcherStop[id] = cancel

	watcher, err := p.dynamicClient.Resource(gvr).Namespace(namespace).Watch(watchCtx, options)
	if err != nil {
		klog.Error(err)
	}
	defer watcher.Stop()
	klog.Info("provider start watcher: ", apis.EventWatchResponseType(gvr), " - ", id)

	for {
		select {
		// TODO: can add a ticker to send all the watch events periodically like informer resync
		case e, ok := <-watcher.ResultChan():
			if !ok {
				klog.Infof("provider watcher is closed, restart a new watcher: %s - %s", apis.MessageWatchResponseType(gvr), id)
				watcher, err = p.dynamicClient.Resource(gvr).Namespace(namespace).Watch(watchCtx, options)
				if err != nil {
					klog.Errorf("failed to restart watcher(%s) with error: %v", id, err)
				}
				continue
			}

			obj, ok := e.Object.(*unstructured.Unstructured)
			if !ok {
				klog.Warning("failed to convert object to unstructured")
				continue
			}
			if p.tweakFunc != nil {
				p.tweakFunc(obj, p.clusterName)
			}

			response := &apis.WatchResponseEvent{
				Type:   e.Type,
				Object: obj,
			}

			evt := cloudevents.NewEvent()
			evt.SetID(string(id))
			evt.SetType(apis.EventWatchResponseType(gvr))
			evt.SetSource(p.clusterName)
			evt.SetData(cloudevents.ApplicationJSON, response)

			klog.Infof("provider send %s", evt.Type())
			utils.PrettyPrint(response)
			result := p.transporter.Send(ctx, evt)
			if cloudevents.IsUndelivered(result) {
				klog.Errorf("failed to send watch response with error: %v", result)
			}
		case <-watchCtx.Done():
			klog.Info("provider cancel watcher: ", apis.EventWatchResponseType(gvr), " - ", id)
			return
		}
	}
}

func (p *genericProvider) sendListResponses(ctx context.Context, id types.UID, namespace string,
	gvr schema.GroupVersionResource, options metav1.ListOptions,
) error {
	unstructuredList, err := p.dynamicClient.Resource(gvr).Namespace(namespace).List(ctx, options)
	if err != nil {
		klog.Errorf("failed to list resource with err: %v", err)
		return err
	}

	if p.tweakFunc != nil {
		for _, obj := range unstructuredList.Items {
			p.tweakFunc(&obj, p.clusterName)
		}
	}

	response := &apis.ListResponseEvent{
		Objects:   unstructuredList,
		EndOfList: true,
	}

	evt := cloudevents.NewEvent()
	evt.SetID(string(id))
	evt.SetType(apis.EventListResponseType(gvr))
	evt.SetSource(p.clusterName)
	evt.SetData(cloudevents.ApplicationJSON, response)

	klog.Infof("provider send %v", evt.Type())
	utils.PrettyPrint(response)
	result := p.transporter.Send(ctx, evt)
	if cloudevents.IsUndelivered(result) {
		klog.Errorf("failed to send list response with error: %v", result)
		return result
	}
	return nil
}
