package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/yanmxa/mqtt-informer/pkg/client"
	"github.com/yanmxa/mqtt-informer/pkg/config"
	"github.com/yanmxa/mqtt-informer/pkg/informers"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	// ctx := context.Background()

	config := config.GetClientConfig()
	client := client.GetClient(config)

	informerFactory := informers.NewSharedMessageInformerFactory(ctx, client, 5*time.Minute,
		config.SignalTopic, config.PayloadTopic)

	gvr := schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	informer := informerFactory.ForResource(gvr)

	restConfig, err := clientcmd.BuildConfigFromFlags("", config.KubeConfig)
	if err != nil {
		panic(err.Error())
	}
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		panic(err.Error())
	}

	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			accessor, _ := meta.Accessor(obj)
			if _, ok := accessor.GetLabels()["mqtt-resource"]; !ok {
				return
			}
			accessor.SetResourceVersion("")
			accessor.SetManagedFields(nil)
			accessor.SetGeneration(0)
			unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(accessor)
			if err != nil {
				klog.Error(err)
				return
			}
			_, err = dynamicClient.Resource(gvr).Namespace(accessor.GetNamespace()).Create(ctx, &unstructured.Unstructured{Object: unstructuredObj}, metav1.CreateOptions{})
			if err != nil {
				klog.Error(err)
				return
			}

			klog.Infof("Added %s/%s", accessor.GetName(), accessor.GetNamespace())
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldAccessor, _ := meta.Accessor(oldObj)
			newAccessor, _ := meta.Accessor(newObj)
			if _, ok := newAccessor.GetLabels()["mqtt-resource"]; !ok {
				return
			}
			unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(newAccessor)
			if err != nil {
				klog.Error(err)
				return
			}

			oldUnstructuredObj, err := dynamicClient.Resource(gvr).Namespace(oldAccessor.GetNamespace()).Get(ctx, oldAccessor.GetName(), metav1.GetOptions{})
			if err != nil {
				klog.Error(err)
				return
			}
			unstructuredObj["metadata"].(map[string]interface{})["resourceVersion"] = oldUnstructuredObj.GetResourceVersion()
			unstructuredObj["metadata"].(map[string]interface{})["uid"] = oldUnstructuredObj.GetUID()
			_, err = dynamicClient.Resource(gvr).Namespace(newAccessor.GetNamespace()).Update(ctx, &unstructured.Unstructured{Object: unstructuredObj}, metav1.UpdateOptions{})
			if err != nil {
				klog.Error(err)
				return
			}
			klog.Infof("Updated from %s/%s to %s/%s", oldAccessor.GetNamespace(), oldAccessor.GetName(), newAccessor.GetNamespace(), newAccessor.GetName())
		},
		DeleteFunc: func(obj interface{}) {
			accessor, _ := meta.Accessor(obj)
			if _, ok := accessor.GetLabels()["mqtt-resource"]; !ok {
				return
			}
			err := dynamicClient.Resource(gvr).Namespace(accessor.GetNamespace()).Delete(ctx, accessor.GetName(), metav1.DeleteOptions{})
			if err != nil {
				klog.Error(err)
				return
			}
			klog.Infof("Deleted %s/%s", accessor.GetName(), accessor.GetNamespace())
		},
	})

	informerFactory.Start()
	<-ctx.Done()
}

func ConvertToUnstructured(obj metav1.Object) (*unstructured.Unstructured, error) {
	// Create an empty unstructured object
	unstructuredObj := &unstructured.Unstructured{}

	// Convert the metav1.Object to a runtime.Object
	runtimeObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}

	// Set the object's fields using the runtime.Object
	unstructuredObj.SetUnstructuredContent(runtimeObj)

	return unstructuredObj, nil
}
