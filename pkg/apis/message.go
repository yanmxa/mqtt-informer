package apis

import (
	"fmt"
	"strings"

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

func toGVRString(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("%s.%s.%s", gvr.Version, gvr.Resource, gvr.Group)
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

func ParseMessageType(t string) (string, schema.GroupVersionResource, error) {
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
