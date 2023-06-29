package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/yanmxa/transport-informer/pkg/config"
	"github.com/yanmxa/transport-informer/pkg/sender"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	clientConfig := config.GetClientConfig()
	transportClient := config.GetMQTTClient(clientConfig)
	restConfig, err := clientcmd.BuildConfigFromFlags("", clientConfig.KubeConfig)
	if err != nil {
		klog.Fatalf("failed to build config, %v", err)
	}
	dynamicClient := dynamic.NewForConfigOrDie(restConfig)

	sender := sender.NewDefaultSender(dynamicClient, transportClient)
	sender.Start(ctx, clientConfig.SignalTopic)
	sender.Send(ctx, clientConfig.PayloadTopic) // this is a blocking call
	sender.Stop()
}
