package provider

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

type listWatcher struct {
	client dynamic.Interface
}

func NewDynamicListWatcher(client dynamic.Interface) ListWatcher {
	return &listWatcher{
		client: client,
	}
}

func (d *listWatcher) List(namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	return d.client.Resource(gvr).Namespace(namespace).List(context.TODO(), options)
}

func (d *listWatcher) Watch(namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) (
	watch.Interface, error,
) {
	return d.client.Resource(gvr).Namespace(namespace).Watch(context.TODO(), options)
}
