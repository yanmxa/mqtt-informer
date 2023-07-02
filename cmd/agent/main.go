package main

import (
	"context"
	"encoding/json"
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
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	informers "github.com/yanmxa/transport-informer/pkg/informer"
	"github.com/yanmxa/transport-informer/pkg/option"
	"github.com/yanmxa/transport-informer/pkg/provider"
	"github.com/yanmxa/transport-informer/pkg/transport"
	"github.com/yanmxa/transport-informer/pkg/utils"
	appsv1 "k8s.io/api/apps/v1"
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

	// start a provider to list/watch local resource and send to transporter
	dynamicClient := dynamic.NewForConfigOrDie(restConfig)
	p := provider.NewDefaultProvider(opt.ClusterName, dynamicClient, transporter,
		opt.ProviderSendTopic, opt.ProviderReceiveTopic,
		func(obj metav1.Object, clusterName string) {
			obj.SetNamespace(clusterName)

			byteObj, err := json.Marshal(obj)
			if err != nil {
				klog.Errorf("marshal object failed: %v", err)
			}
			labels := obj.GetLabels()
			if labels == nil {
				labels = map[string]string{}
			}
			labels["object"] = string(byteObj)
			obj.SetLabels(labels)
		})
	go p.Run(ctx)

	gvr := schema.GroupVersionResource{
		Group:    "apps",
		Version:  "v1",
		Resource: "deployments",
	}

	// wait until the provider is ready
	// provider.WaitUntilProviderReady(ctx, transporter, opt.InformerSendTopic, opt.InformerReceiveTopic, gvr)

	// 1. informer to list/watch response from transporter, and then apply resource to local cluster
	// 2. only care about the resource with cluster target namespace
	informerFactory := informers.NewSharedMessageInformerFactory(ctx, transporter, time.Minute*5,
		opt.InformerSendTopic, opt.InformerReceiveTopic, opt.ClusterName, nil)

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
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		panic(err.Error())
	}
	// sharedIndexInformer := informer.Informer()
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			accessor, _ := meta.Accessor(obj)
			validateNamespace(kubeClient, accessor.GetNamespace())

			namespace := accessor.GetNamespace()
			lastAppliedConfigJSON := accessor.GetAnnotations()["kubectl.kubernetes.io/last-applied-configuration"]
			deployment := &appsv1.Deployment{}
			err = json.Unmarshal([]byte(lastAppliedConfigJSON), deployment)
			if err != nil {
				panic(err.Error())
			}

			// pay, _ := json.MarshalIndent(deployment, "", "  ")
			// klog.Infof("create deployment: %s", string(pay))
			deployment.SetResourceVersion("")
			deployment.SetUID("")
			deployment.SetGeneration(0)
			deployment.SetManagedFields(nil)
			deployment.SetNamespace(namespace)
			_, err = kubeClient.AppsV1().Deployments(accessor.GetNamespace()).Create(context.Background(), deployment, metav1.CreateOptions{})
			// _, err = dynamicClient.Resource(gvr).Namespace(accessor.GetNamespace()).Create(ctx, &unstructured.Unstructured{Object: unstructuredObj}, metav1.CreateOptions{})
			if errors.IsAlreadyExists(err) {
				klog.Infof("already exists %s/%s", accessor.GetNamespace(), accessor.GetName())
				return
			} else if err != nil {
				klog.Errorf("create %s/%s failed: %v", accessor.GetNamespace(), accessor.GetName(), err)
				return
			}

			klog.Infof("Added %s/%s", accessor.GetNamespace(), accessor.GetName())
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			// oldAccessor, _ := meta.Accessor(oldObj)
			newAccessor, _ := meta.Accessor(newObj)

			objJSON := newAccessor.GetLabels()["object"]
			deployment := &appsv1.Deployment{}
			err = json.Unmarshal([]byte(objJSON), deployment)
			if err != nil {
				panic(err.Error())
			}

			oldUnstructured, err := dynamicClient.Resource(gvr).Namespace(newAccessor.GetNamespace()).Get(ctx, newAccessor.GetName(), metav1.GetOptions{})
			if errors.IsNotFound(err) {
				createDeploy := &appsv1.Deployment{}
				createDeploy.SetName(deployment.GetName())
				createDeploy.SetNamespace(deployment.GetNamespace())
				createDeploy.SetLabels(deployment.GetLabels())
				createDeploy.SetAnnotations(deployment.GetAnnotations())
				createDeploy.Spec = deployment.Spec
				_, err := kubeClient.AppsV1().Deployments(deployment.GetNamespace()).Create(ctx, createDeploy, metav1.CreateOptions{})
				if err != nil {
					klog.Error(err)
				}
				klog.Infof("create %s/%s", newAccessor.GetNamespace(), newAccessor.GetName())
				return
			} else if err != nil {
				klog.Infof("get %s/%s failed: %v", newAccessor.GetNamespace(), newAccessor.GetName(), err)
				return
			}

			oldUnstructuredObj := oldUnstructured.Object

			oldUnstructuredObj["labels"] = deployment.Labels
			oldUnstructuredObj["annotations"] = deployment.Annotations
			spec := oldUnstructuredObj["spec"].(map[string]interface{})
			spec["replicas"] = *deployment.Spec.Replicas
			oldUnstructuredObj["spec"] = spec

			_, err = dynamicClient.Resource(gvr).Namespace(newAccessor.GetNamespace()).Update(ctx, &unstructured.Unstructured{Object: oldUnstructuredObj}, metav1.UpdateOptions{})
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

func convertUnstructuredToDeployment(unstructuredObj *unstructured.Unstructured) (*appsv1.Deployment, error) {
	deploymentObj := &appsv1.Deployment{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.Object, deploymentObj)
	if err != nil {
		return nil, err
	}

	return deploymentObj, nil
}
