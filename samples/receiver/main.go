package main

import (
	"fmt"
	"os"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/yanmxa/transport-informer/pkg/option"
	"github.com/yanmxa/transport-informer/pkg/transport"
)

func main() {
	opt := option.ParseOptionFromFlag()
	transporter := transport.NewMqttTransport(opt)
	client := transporter.GetClient()

	choke := make(chan [2]string)
	// opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
	// 	choke <- [2]string{msg.Topic(), string(msg.Payload())}
	// })

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	if token := client.Subscribe(opt.PayloadTopic, opt.QoS,
		func(client MQTT.Client, msg MQTT.Message) {
			// fmt.Println("received message: ", msg.Topic(), string(msg.Payload()))
			choke <- [2]string{msg.Topic(), string(msg.Payload())}
		}); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	receiveCount := 0
	for receiveCount < 4 {
		incoming := <-choke
		fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
		receiveCount++
	}
	client.Disconnect(250)
	fmt.Println("Subscriber Disconnected")
}
