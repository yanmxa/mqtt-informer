package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	informers "github.com/yanmxa/straw/pkg/informer"
	"github.com/yanmxa/straw/pkg/option"
	"github.com/yanmxa/straw/pkg/transport"
	"github.com/yanmxa/straw/pkg/utils"
)

func init() {
	klog.SetLogger(utils.DefaultLogger())
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	opt := option.ParseOptionFromFlag()

	transportClient, err := transport.CloudeventsClient(ctx, opt, transport.Informer)
	if err != nil {
		log.Fatal(err)
	}

	// only informer the resource with label "mqtt-resource"
	informerFactory := informers.NewSharedMessageInformerFactory(ctx, transporter, time.Minute*5,
		opt.InformerSendTopic, opt.InformerReceiveTopic, metav1.NamespaceAll, func(options *metav1.ListOptions) {
			options.LabelSelector = fmt.Sprintf("%s=", "mqtt-resource")
		})

	gvr := schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	secretInformer := informerFactory.ForResource(gvr)

	restConfig, err := clientcmd.BuildConfigFromFlags("", opt.KubeConfig)
	if err != nil {
		panic(err.Error())
	}
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		panic(err.Error())
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		panic(err.Error())
	}

	// sharedIndexInformer := informer.Informer()
	secretInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			accessor, _ := meta.Accessor(obj)
			validateNamespace(kubeClient, accessor.GetNamespace())

			accessor.SetResourceVersion("")
			accessor.SetManagedFields(nil)
			accessor.SetGeneration(0)
			unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(accessor)
			if err != nil {
				klog.Error(err)
				return
			}
			_, err = dynamicClient.Resource(gvr).Namespace(accessor.GetNamespace()).Create(ctx, &unstructured.Unstructured{Object: unstructuredObj}, metav1.CreateOptions{})
			if errors.IsAlreadyExists(err) {
				klog.Infof("Already exists %s/%s", accessor.GetNamespace(), accessor.GetName())
				return
			}

			klog.Infof("Added %s/%s", accessor.GetNamespace(), accessor.GetName())
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldAccessor, _ := meta.Accessor(oldObj)
			newAccessor, _ := meta.Accessor(newObj)
			oldUnstructuredObj, err := dynamicClient.Resource(gvr).Namespace(oldAccessor.GetNamespace()).Get(ctx, oldAccessor.GetName(), metav1.GetOptions{})
			if err != nil {
				klog.Error(err)
				return
			}
			newUnstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(newAccessor)
			if err != nil {
				klog.Error(err)
				return
			}
			newUnstructuredObj["metadata"].(map[string]interface{})["resourceVersion"] = oldUnstructuredObj.GetResourceVersion()
			newUnstructuredObj["metadata"].(map[string]interface{})["uid"] = oldUnstructuredObj.GetUID()

			_, err = dynamicClient.Resource(gvr).Namespace(newAccessor.GetNamespace()).Update(ctx, &unstructured.Unstructured{Object: newUnstructuredObj}, metav1.UpdateOptions{})
			if err != nil {
				klog.Error(err)
				return
			}
			klog.Infof("Updated %s/%s", newAccessor.GetNamespace(), newAccessor.GetName())
		},
		DeleteFunc: func(obj interface{}) {
			accessor, _ := meta.Accessor(obj)
			err := dynamicClient.Resource(gvr).Namespace(accessor.GetNamespace()).Delete(ctx, accessor.GetName(), metav1.DeleteOptions{})
			if err != nil {
				klog.Error(err)
				return
			}
			klog.Infof("Deleted %s/%s", accessor.GetNamespace(), accessor.GetName())
		},
	})

	informerFactory.Start()
	<-ctx.Done()
	time.Sleep(2 * time.Second) // wait for the informer send stop signal to transporter
	transporter.Stop()
}

func validateNamespace(kubeClient *kubernetes.Clientset, namespace string) error {
	_, err := kubeClient.CoreV1().Namespaces().Get(context.Background(), namespace, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		_, err = kubeClient.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			},
		}, metav1.CreateOptions{})
	}

	return err
}
