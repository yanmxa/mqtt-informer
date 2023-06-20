package main

import (
	"log"
	"time"

	"github.com/yanmxa/mqtt-informer/pkg/client"
	"github.com/yanmxa/mqtt-informer/pkg/config"
)

func main() {
	sendConfig := config.GetClientConfig()
	c := client.GetClient(sendConfig)

	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	qos := 0 // Quality of Service (QoS) level

	token := c.Publish("my", byte(qos), false, []byte("{Hello: MQTT!}"))
	token.WaitTimeout(3 * time.Second)

	if token.Error() != nil {
		log.Fatal(token.Error())
	}
	c.Disconnect(250)
}
