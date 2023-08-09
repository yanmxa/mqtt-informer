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
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

// Reflector watches a specified resource and causes all changes to be reflected in the given store.
type ReflectorFactory struct {
	clusters map[string]*ClusterRefactor
	running  map[string]bool
	mutex    sync.RWMutex
}

func NewReflectorFactory() *ReflectorFactory {
	return &ReflectorFactory{
		clusters: map[string]*ClusterRefactor{},
		running:  map[string]bool{},
	}
}

func (r *ReflectorFactory) ForClusterResource(cluster string, gvr schema.GroupVersionResource) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.clusters[cluster] = NewClusterReflector(cluster, "", nil, nil, nil)
	r.running[cluster] = false
}

// Run repeatedly uses the reflector's ListAndWatch to fetch all the
// objects and subsequent deltas.
// Run will exit when stopCh is closed.
func (r *ReflectorFactory) Run(stopCh <-chan struct{}) {
	klog.V(3).Infof("Starting reflector %s (%s) from %s", r.typeDescription, r.resyncPeriod, r.name)
	wait.BackoffUntil(func() {
		select {
		case <-stopCh:
			return
		default:
			for name, clusterRefactor := range r.clusters {
				if !r.running[name] {
					// if err := cluster.listAndWatch(stopCh); err != nil {
					// 	r.watchErrorHandler(r, err)
					// }
					r.mutex.Lock()
					go clusterRefactor.Run(stopCh)
					r.running[name] = true
					r.mutex.Unlock()
				}
			}
		}
	}, r.backoffManager, true, stopCh)
	klog.V(3).Infof("Stopping reflector %s (%s) from %s", r.typeDescription, r.resyncPeriod, r.name)
}
