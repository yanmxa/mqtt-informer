package main

import (
	"log"
	"time"

	"github.com/yanmxa/transport-informer/pkg/option"
	"github.com/yanmxa/transport-informer/pkg/transport"
)

func main() {
	opt := option.ParseOptionFromFlag()
	transporter := transport.NewMqttTransport(opt)
	c := transporter.GetClient()

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
