package main

import (
	"fmt"
	"os"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/yanmxa/mqtt-informer/pkg/config"
	"github.com/yanmxa/mqtt-informer/pkg/utils"
)

func main() {
	receiverConfig := config.GetClientConfig()
	opts := MQTT.NewClientOptions()
	opts.AddBroker(receiverConfig.Broker)
	opts.SetClientID(receiverConfig.ClientID)
	opts.SetTLSConfig(utils.NewTLSConfig(receiverConfig.CACert, receiverConfig.ClientCert,
		receiverConfig.ClientKey))

	choke := make(chan [2]string)
	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		choke <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	fmt.Println("Subscriber Starting")
	if token := client.Subscribe(receiverConfig.Topic, receiverConfig.QoS,
		// func(client MQTT.Client, msg MQTT.Message) {
		// 	fmt.Println("received message: ", msg.Topic(), string(msg.Payload()))
		// }
		nil); token.Wait() && token.Error() != nil {
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
