package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/yanmxa/mqtt-informer/pkg/client"
	"github.com/yanmxa/mqtt-informer/pkg/config"
	"github.com/yanmxa/mqtt-informer/pkg/senders"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	clientConfig := config.GetClientConfig()
	client := client.GetClient(clientConfig)

	restConfig, err := clientcmd.BuildConfigFromFlags("", clientConfig.KubeConfig)
	if err != nil {
		klog.Fatalf("failed to build config, %v", err)
	}

	dynamicClient := dynamic.NewForConfigOrDie(restConfig)
	s := senders.NewDynamicSender(dynamicClient)
	transport := senders.NewDefaultSenderTransport(s, client)

	transport.Run(ctx)
}
