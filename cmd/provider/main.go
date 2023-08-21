package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/yanmxa/straw/pkg/apis"
	"github.com/yanmxa/straw/pkg/option"
	"github.com/yanmxa/straw/pkg/provider"
	"github.com/yanmxa/straw/pkg/transport"
	"github.com/yanmxa/straw/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

//  ./bin/provider --broker 127.0.0.1:1883 --client-id spoke-cluster-id --send-topic /event/payload --receive-topic /event/signal --cluster cluster1

func init() {
	klog.SetLogger(utils.DefaultLogger())
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	opt := option.ParseOptionFromFlag()

	transportClient, err := transport.CloudeventsClient(ctx, opt)
	if err != nil {
		log.Fatal(err)
	}

	// start the provider to handle the request from hub cluster
	go func() {
		restConfig, err := clientcmd.BuildConfigFromFlags("", opt.KubeConfig)
		if err != nil {
			klog.Fatalf("failed to build config, %v", err)
		}
		dynamicClient := dynamic.NewForConfigOrDie(restConfig)

		p := provider.NewProvider(opt.ClusterName, dynamicClient, transportClient,
			func(obj metav1.Object, clusterName string) {
				labels := obj.GetLabels()
				if labels == nil {
					labels = map[string]string{}
				}
				labels[utils.ClusterLabelKey] = clusterName
			})

		err = p.Run(ctx)
		if err != nil {
			klog.Fatalf("provider shut down with error: %v", err)
		}
	}()

	// heartbeat to send the signal to hub cluster
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case <-ticker.C:
			err := transportClient.Send(ctx, apis.NewHeartbeatEvent(opt.ClusterName))
			if err != nil {
				klog.Errorf("failed to send heartbeat event")
			}
		}
	}
	klog.Info("provider shut down gracefully")
	time.Sleep(5 * time.Second)
}
