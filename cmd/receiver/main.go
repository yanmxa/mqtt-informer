package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/yanmxa/mqtt-informer/pkg/client"
	"github.com/yanmxa/mqtt-informer/pkg/config"
	"github.com/yanmxa/mqtt-informer/pkg/informers"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	sendConfig, receiveConfig := config.GetConfigs()
	sender := client.GetSender(sendConfig)
	receiver := client.GetReceiver(receiveConfig)

	informerFactory := informers.NewSharedMessageInformerFactory(sender, receiver, 5*time.Minute)
	informer := informerFactory.ForResource(schema.GroupVersionResource{Version: "v1", Resource: "secrets"})
	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			accessor, _ := meta.Accessor(obj)
			klog.Infof("added %s/%s", accessor.GetName(), accessor.GetNamespace())
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldAccessor, _ := meta.Accessor(oldObj)
			newAccessor, _ := meta.Accessor(newObj)
			klog.Infof("Updated from %s/%s to %s/%s", oldAccessor.GetNamespace(), oldAccessor.GetName(), newAccessor.GetNamespace(), newAccessor.GetName())
		},
		DeleteFunc: func(obj interface{}) {
			klog.Infof("deleted %v", obj)
		},
	})

	informerFactory.Start()
	<-ctx.Done()
}
