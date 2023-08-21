/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package refactor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/yanmxa/straw/pkg/apis"
)

// Reflector watches a specified resource and causes all changes to be reflected in the given store.
type ReflectorFactory struct {
	ctx        context.Context
	clusters   map[string]*ClusterRefactor
	deltaQueue cache.Queue
	mutex      sync.RWMutex
	transport  cloudevents.Client
	gvr        schema.GroupVersionResource
}

func NewReflectorFactory(ctx context.Context, gvr schema.GroupVersionResource, t cloudevents.Client) *ReflectorFactory {
	fifo := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KnownObjects:          cache.NewIndexer(cache.DeletionHandlingMetaNamespaceKeyFunc, nil),
		EmitDeltaTypeReplaced: true,
		KeyFunction:           ClusterMetaNamespaceKeyFunc,
		// Transformer:           s.transform,
	})
	return &ReflectorFactory{
		ctx:        ctx,
		clusters:   map[string]*ClusterRefactor{},
		transport:  t,
		gvr:        gvr,
		deltaQueue: fifo,
	}
}

// Run repeatedly uses the reflector's ListAndWatch to fetch all the
// objects and subsequent deltas.
// Run will exit when stopCh is closed.
func (r *ReflectorFactory) Run(stopCh <-chan struct{}) {
	klog.V(3).Infof("Starting ReflectorFactory %s", apis.ToGVRString(r.gvr))
	// start a goroutine to manage the cluster reflector
	// this goroutine will receive the register and unregister event
	go func() {
		r.transport.StartReceiver(r.ctx, func(event cloudevents.Event) error {
			switch event.Type() {
			case string(apis.ModeRegister):
				klog.V(3).Infof("Registering reflector %s", event.Source())
				r.RegisterRefactor(event.Source())
			case string(apis.ModeUnregister):
				klog.V(3).Infof("Unregistering reflector %s", event.Source())
				r.UnregisterRefactor(event.Source())
			}
			return nil
		})
	}()

	// consume the obj from delta queues
	go func() {
		<-stopCh
		r.deltaQueue.Close()
	}()
	go wait.Until(r.processLoop, time.Second, stopCh)

	// refactor resource from cluster
	wait.Until(func() {
		for _, clusterRefactor := range r.clusters {
			if !clusterRefactor.IsRunning {
				klog.V(3).Infof("Starting reflector %s", clusterRefactor.name)
				go clusterRefactor.Run(stopCh)
			}
		}
	}, 5*time.Second, stopCh)

	klog.V(3).Infof("Stopping ReflectorFactory %s", apis.ToGVRString(r.gvr))
}

func (r *ReflectorFactory) RegisterRefactor(cluster string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.clusters[cluster] = NewClusterReflector(cluster, "", nil, nil, r.deltaQueue)
}

func (r *ReflectorFactory) UnregisterRefactor(cluster string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.clusters[cluster].StopRefactor()
	delete(r.clusters, cluster)
}

// processLoop drains the work queue.
// TODO: Consider doing the processing in parallel. This will require a little thought
// to make sure that we don't end up processing the same object multiple times
// concurrently.
//
// TODO: Plumb through the stopCh here (and down to the queue) so that this can
// actually exit when the controller is stopped. Or just give up on this stuff
// ever being stoppable. Converting this whole package to use Context would
// also be helpful.
func (r *ReflectorFactory) processLoop() {
	for {
		_, err := r.deltaQueue.Pop(cache.PopProcessFunc(r.handleDeltas))
		if err != nil {
			if err == cache.ErrFIFOClosed {
				return
			}
			klog.Errorf("Unable to pop from deltas queue: %v", err)
			// consider adding retry logic here
			// if c.config.RetryOnError {
			// 	// This is the safe way to re-enqueue.
			// 	c.config.Queue.AddIfNotPresent(obj)
			// }
		}
	}
}

func (r *ReflectorFactory) handleDeltas(obj interface{}, isInInitialList bool) error {
	if deltas, ok := obj.(Deltas); ok {
		return processDeltas(nil, nil, deltas, isInInitialList)
	}
	return errors.New("object given as Process argument is not Deltas")
}

var RetryOnError bool

// Multiplexes updates in the form of a list of Deltas into a Store, and informs
// a given handler of events OnUpdate, OnAdd, OnDelete
func processDeltas(
	// Object which receives event notifications from the given deltas
	handler cache.ResourceEventHandler,
	clientState Store,
	deltas Deltas,
	isInInitialList bool,
) error {
	// from oldest to newest
	for _, d := range deltas {
		obj := d.Object

		switch d.Type {
		case Sync, Replaced, Added, Updated:
			if old, exists, err := clientState.Get(obj); err == nil && exists {
				if err := clientState.Update(obj); err != nil {
					return err
				}
				// handler.OnUpdate(old, obj)
				fmt.Println("OnUpdate", "old", old, "new", obj)
			} else {
				if err := clientState.Add(obj); err != nil {
					return err
				}
				// handler.OnAdd(obj, isInInitialList)
				fmt.Println("OnAdd", "new", obj)
			}
		case Deleted:
			if err := clientState.Delete(obj); err != nil {
				return err
			}
			// handler.OnDelete(obj)
			fmt.Println("OnDelete", "old", obj)
		}
	}
	return nil
}
