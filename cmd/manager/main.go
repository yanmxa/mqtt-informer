package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/yanmxa/transport-informer/pkg/informer"
	"github.com/yanmxa/transport-informer/pkg/option"
	"github.com/yanmxa/transport-informer/pkg/provider"
	"github.com/yanmxa/transport-informer/pkg/transport"
	"github.com/yanmxa/transport-informer/pkg/utils"
)

func init() {
	klog.SetLogger(utils.DefaultLogger())
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	opt := option.ParseOptionFromFlag()
	restConfig, err := clientcmd.BuildConfigFromFlags("", opt.KubeConfig)
	if err != nil {
		panic(err.Error())
	}
	// transport for both informer and provider
	transporter := transport.NewMqttTransport(ctx, opt)

	// start a provider to list/watch local resource to transporter
	// the agent will wait until the provider is ready
	dynamicClient := dynamic.NewForConfigOrDie(restConfig)
	p := provider.NewDefaultProvider(utils.HubClusterName, dynamicClient, transporter,
		opt.ProviderSendTopic, opt.ProviderReceiveTopic,
		func(obj metav1.Object, clusterName string) {
			labels := obj.GetLabels()
			if labels == nil {
				labels = map[string]string{}
			}
			targetNamespace := labels[utils.ClusterTargetNamespaceLabelKey]
			if targetNamespace == "" {
				klog.Info("target namespace is empty, use cluster name as target namespace")
				targetNamespace = clusterName
			}
			obj.SetNamespace(targetNamespace)

			annotations := obj.GetAnnotations()
			delete(annotations, fmt.Sprintf("cluster.available.replicas/%s", targetNamespace))

			byteObj, err := json.Marshal(obj)
			if err != nil {
				klog.Errorf("marshal object failed: %v", err)
			}
			labels["object"] = string(byteObj)
			obj.SetLabels(labels)
			obj.SetAnnotations(annotations)
		})
	go p.Run(ctx)

	// only cluster informer is ready to go
	// todo: if a cluster is registered, then how to let the informer know the cluster is ready? and how to let list/watch the resource from the cluster?
	// provider.WaitUntilProviderReady(ctx, transporter, opt.InformerSendTopic, opt.InformerReceiveTopic, gvr)

	// informer to list/watch response from transporter, and then apply resource to local cluster
	// only care about the resource with cluster target namespace:
	//      the namespace and tweakListOptionsFunc will be propagate to the provider

	gvr := schema.GroupVersionResource{
		Group:    "apps",
		Version:  "v1",
		Resource: "deployments",
	}
	informerFactory := informer.NewSharedMessageInformerFactory(ctx, transporter, time.Minute*5,
		opt.InformerSendTopic, opt.InformerReceiveTopic, metav1.NamespaceAll, func(options *metav1.ListOptions) {
			options.LabelSelector = fmt.Sprintf("%s=", utils.TransportResourceLabelKey)
		})

	deployInformer := informerFactory.ForResource(gvr)
	addInformerHandler(ctx, deployInformer.Informer(), restConfig, gvr)
	informerFactory.Start()

	<-ctx.Done()
	time.Sleep(2 * time.Second) // wait for the informer send stop signal to transporter
	transporter.Stop()
}

func addInformerHandler(ctx context.Context, informer cache.SharedIndexInformer, restConfig *rest.Config, gvr schema.GroupVersionResource) {
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		panic(err.Error())
	}
	// kubeClient, err := kubernetes.NewForConfig(restConfig)
	// if err != nil {
	// 	panic(err.Error())
	// }

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			accessor, _ := meta.Accessor(obj)

			objJSON := accessor.GetLabels()["object"]
			deployment := &appsv1.Deployment{}
			err := json.Unmarshal([]byte(objJSON), deployment)
			if err != nil {
				klog.Error(err)
			}
			replicas := deployment.Status.AvailableReplicas

			oldUnstructured, err := dynamicClient.Resource(gvr).Namespace(accessor.GetNamespace()).Get(ctx, accessor.GetName(), metav1.GetOptions{})
			if err != nil {
				klog.Infof("get %s/%s failed: %v", accessor.GetNamespace(), accessor.GetName(), err)
				return
			}
			annotations := oldUnstructured.GetAnnotations()
			annotations[fmt.Sprintf("cluster.available.replicas/%s", accessor.GetNamespace())] = fmt.Sprintf("%d", replicas)
			oldUnstructured.SetAnnotations(annotations)

			_, err = dynamicClient.Resource(gvr).Namespace(accessor.GetNamespace()).Update(ctx, oldUnstructured, metav1.UpdateOptions{})
			if err != nil {
				klog.Error(err)
				return
			}
			klog.Infof("Added %s/%s: ", accessor.GetNamespace(), accessor.GetName())
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			// oldAccessor, _ := meta.Accessor(oldObj)
			newAccessor, _ := meta.Accessor(newObj)

			accessor := newAccessor
			objJSON := accessor.GetLabels()["object"]
			deployment := &appsv1.Deployment{}
			err := json.Unmarshal([]byte(objJSON), deployment)
			if err != nil {
				klog.Error(err)
			}
			replicas := deployment.Status.AvailableReplicas

			oldUnstructured, err := dynamicClient.Resource(gvr).Namespace(accessor.GetNamespace()).Get(ctx, accessor.GetName(), metav1.GetOptions{})
			if err != nil {
				klog.Infof("get %s/%s failed: %v", accessor.GetNamespace(), accessor.GetName(), err)
				return
			}
			annotations := oldUnstructured.GetAnnotations()
			annotations[fmt.Sprintf("cluster.available.replicas/%s", accessor.GetNamespace())] = fmt.Sprintf("%d", replicas)
			oldUnstructured.SetAnnotations(annotations)

			_, err = dynamicClient.Resource(gvr).Namespace(accessor.GetNamespace()).Update(ctx, oldUnstructured, metav1.UpdateOptions{})
			if err != nil {
				klog.Error(err)
			}
			klog.Infof("Updated %s/%s ", accessor.GetNamespace(), accessor.GetName())
		},
		DeleteFunc: func(obj interface{}) {
			accessor, _ := meta.Accessor(obj)
			// err := dynamicClient.Resource(gvr).Namespace(accessor.GetNamespace()).Delete(ctx, accessor.GetName(), metav1.DeleteOptions{})
			// if err != nil {
			// 	klog.Error(err)
			// 	return
			// }
			oldUnstructured, err := dynamicClient.Resource(gvr).Namespace(accessor.GetNamespace()).Get(ctx, accessor.GetName(), metav1.GetOptions{})
			if err != nil {
				klog.Infof("get %s/%s failed: %v", accessor.GetNamespace(), accessor.GetName(), err)
				return
			}
			annotations := oldUnstructured.GetAnnotations()
			annotations[fmt.Sprintf("cluster.available.replicas/%s", accessor.GetNamespace())] = fmt.Sprintf("%d", 0)
			oldUnstructured.SetAnnotations(annotations)

			_, err = dynamicClient.Resource(gvr).Namespace(accessor.GetNamespace()).Update(ctx, oldUnstructured, metav1.UpdateOptions{})
			if err != nil {
				klog.Error(err)
				return
			}
			klog.Infof("Deleted %s/%s", accessor.GetNamespace(), accessor.GetName())
		},
	})
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
