package provider

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

type ListWatcher interface {
	List(namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) (*unstructured.UnstructuredList, error)
	Watch(namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) (watch.Interface, error)
}

type Provider interface {
	// Run blocks until the context is done.
	Run(ctx context.Context) error
}
