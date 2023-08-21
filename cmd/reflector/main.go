package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	"github.com/yanmxa/straw/pkg/option"
	"github.com/yanmxa/straw/pkg/reflector"
	"github.com/yanmxa/straw/pkg/transport"
	"github.com/yanmxa/straw/pkg/utils"
)

// ./bin/reflector --broker 127.0.0.1:1883 --client-id hub-cluster-id --send-topic /event/signal --receive-topic /event/payload

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

	gvr := schema.GroupVersionResource{Version: "v1", Resource: "secrets"}

	refactorFactory := reflector.NewReflectorFactory(ctx, gvr, transportClient)

	stopChan := make(chan struct{})
	defer close(stopChan)
	go func() {
		<-ctx.Done()
		close(stopChan)
	}()

	refactorFactory.Run(stopChan)
	time.Sleep(2 * time.Second) // wait for the informer send stop signal to transporter
}
