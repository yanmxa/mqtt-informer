package apis

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

type RequestEvent struct {
	Namespace string             `json:"namespace"`
	Options   metav1.ListOptions `json:"options"`
}
type ListResponseEvent struct {
	Objects   *unstructured.UnstructuredList `json:"objects"`
	EndOfList bool                           `json:"endOfList"`
}

func EventListResponseType(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("response.list.%s", ToGVRString(gvr))
}

type WatchResponseEvent struct {
	Type   watch.EventType            `json:"type"`
	Object *unstructured.Unstructured `json:"object"`
}

func EventWatchResponseType(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("response.watch.%s", ToGVRString(gvr))
}

func ToGVRString(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("%s.%s.%s", gvr.Version, gvr.Resource, gvr.Group)
}

func EventListType(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("list.%s", ToGVRString(gvr))
}

func EventWatchType(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("watch.%s", ToGVRString(gvr))
}

func EventStopWatchType(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("stopwatch.%s", ToGVRString(gvr))
}

func ParseEventType(t string) (string, schema.GroupVersionResource, error) {
	eventTypeArray := strings.Split(t, ".")
	if len(eventTypeArray) != 4 {
		return "", schema.GroupVersionResource{}, fmt.Errorf("failed to parse message type")
	}

	return eventTypeArray[0], schema.GroupVersionResource{
		Version:  eventTypeArray[1],
		Resource: eventTypeArray[2],
		Group:    eventTypeArray[3],
	}, nil
}
