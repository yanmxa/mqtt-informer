package apis

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

type Mode string

const (
	ModeList  Mode = "list"
	ModeWatch Mode = "watch"
	ModeStop  Mode = "stopwatch"
)

type TransportMessage struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Source  string `json:"source"`
	Payload []byte `json:"payload"`
}

type RequestMessage struct {
	Namespace string             `json:"namespace"`
	Options   metav1.ListOptions `json:"options"`
}

type ListResponseMessage struct {
	Objects   *unstructured.UnstructuredList `json:"objects"`
	EndOfList bool                           `json:"endOfList"`
}

type WatchResponseMessage struct {
	Type   watch.EventType            `json:"type"`
	Object *unstructured.Unstructured `json:"object"`
}

func MessageListType(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("list.%s", toGVRString(gvr))
}

func MessageWatchType(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("watch.%s", toGVRString(gvr))
}

func MessageStopWatchType(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("stopwatch.%s", toGVRString(gvr))
}

func MessageListResponseType(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("response.list.%s", toGVRString(gvr))
}

func MessageWatchResponseType(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("response.watch.%s", toGVRString(gvr))
}
