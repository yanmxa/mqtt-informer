package informer

import (
	"context"
	"time"

	"github.com/yanmxa/straw/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/metadata/metadatalister"
	"k8s.io/client-go/tools/cache"
)

type messageInformer struct {
	informer cache.SharedIndexInformer
	gvr      schema.GroupVersionResource
}

// NewFilteredMetadataInformer constructs a new informer for a metadata type.
func NewFilteredMetadataInformer(ctx context.Context, t transport.Transport, gvr schema.GroupVersionResource,
	namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakOptions TweakListOptionsFunc,
	sendTopic, receiveTopic string,
) informers.GenericInformer {
	lw := NewMessageListWatcher(ctx, t, namespace, gvr, sendTopic, receiveTopic)

	return &messageInformer{
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

func (d *messageInformer) Informer() cache.SharedIndexInformer {
	return d.informer
}

func (d *messageInformer) Lister() cache.GenericLister {
	return metadatalister.NewRuntimeObjectShim(metadatalister.New(d.informer.GetIndexer(), d.gvr))
}
