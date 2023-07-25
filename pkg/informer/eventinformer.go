package informer

import (
	"context"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/metadata/metadatalister"
	"k8s.io/client-go/tools/cache"
)

var _ informers.GenericInformer = (*eventInformer)(nil)

type eventInformer struct {
	informer cache.SharedIndexInformer
	gvr      schema.GroupVersionResource
}

// NewFilteredEventInformer constructs a new informer for a metadata type.
func NewFilteredEventInformer(ctx context.Context, t cloudevents.Client, gvr schema.GroupVersionResource,
	namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakOptions TweakListOptionsFunc,
) informers.GenericInformer {
	lw := NewEventListWatcher(ctx, t, namespace, gvr, "informer")
	return &eventInformer{
		gvr: gvr,
		informer: cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					if tweakOptions != nil {
						tweakOptions(&options)
					}
					return lw.List(options)
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					if tweakOptions != nil {
						tweakOptions(&options)
					}
					return lw.Watch(options)
				},
			},
			&metav1.PartialObjectMetadata{},
			resyncPeriod,
			indexers,
		),
	}
}

func (d *eventInformer) Informer() cache.SharedIndexInformer {
	return d.informer
}

func (d *eventInformer) Lister() cache.GenericLister {
	return metadatalister.NewRuntimeObjectShim(metadatalister.New(d.informer.GetIndexer(), d.gvr))
}
