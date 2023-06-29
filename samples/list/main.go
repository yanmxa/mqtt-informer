package main

import (
	"encoding/json"
	"fmt"

	"github.com/yanmxa/transport-informer/pkg/apis"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func main() {
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	requestListType := apis.MessageListType(gvr)
	msg := &apis.TransportMessage{}
	msg.Type = requestListType
	msg.ID = "1"
	msg.Payload = []byte(`{"namespace":"default"}`)
	msg.Source = "hub"
	m, e := json.Marshal(msg)
	if e != nil {
		fmt.Println(e)
	}
	fmt.Println(string(m))
}
