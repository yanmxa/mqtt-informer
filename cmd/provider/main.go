package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/yanmxa/straw/pkg/option"
	"github.com/yanmxa/straw/pkg/provider"
	"github.com/yanmxa/straw/pkg/transport"
	"github.com/yanmxa/straw/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	klog.SetLogger(utils.DefaultLogger())
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	opt := option.ParseOptionFromFlag()

	transportClient, err := transport.CloudeventsClient(ctx, opt, transport.Provider)
	if err != nil {
		log.Fatal(err)
	}

	restConfig, err := clientcmd.BuildConfigFromFlags("", opt.KubeConfig)
	if err != nil {
		klog.Fatalf("failed to build config, %v", err)
	}
	dynamicClient := dynamic.NewForConfigOrDie(restConfig)

	p := provider.NewProvider(opt.ClusterName, dynamicClient, transportClient,
		func(obj metav1.Object, clusterName string) {
			// name := obj.GetName()
			// namespace := obj.GetNamespace()
			// if namespace == "" {
			// 	obj.SetName(clusterName + "." + name)
			// } else {
			// 	obj.SetName(namespace + "." + name)
			// 	obj.SetNamespace(clusterName)
			// }
			// obj.SetManagedFields(nil)
		})

	err = p.Run(ctx)
	if err != nil {
		klog.Fatalf("provider shut down with error: %v", err)
	}
	klog.Info("provider shut down gracefully")
}
