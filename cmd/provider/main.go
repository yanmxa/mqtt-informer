package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/yanmxa/transport-informer/pkg/option"
	"github.com/yanmxa/transport-informer/pkg/provider"
	"github.com/yanmxa/transport-informer/pkg/transport"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	opt := option.ParseOptionFromFlag()

	restConfig, err := clientcmd.BuildConfigFromFlags("", opt.KubeConfig)
	if err != nil {
		klog.Fatalf("failed to build config, %v", err)
	}
	dynamicClient := dynamic.NewForConfigOrDie(restConfig)

	transporter := transport.NewMqttTransport(ctx, opt)

	p := provider.NewDefaultProvider(opt.ClusterName, dynamicClient, transporter)

	p.Run(ctx)
	transporter.Stop()
}
