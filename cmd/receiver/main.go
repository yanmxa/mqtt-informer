package main

import (
	goflag "flag"
	"fmt"
	"os"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	flag "github.com/spf13/pflag"

	"github.com/yanmxa/mqtt-informer/pkg/config"
	"github.com/yanmxa/mqtt-informer/pkg/utils"
)

func main() {
	receiverConfig := config.NewReceiveConfig()
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	flag.StringVarP(&receiverConfig.Broker, "broker", "b", "", "the MQTT server")
	flag.BoolVarP(&receiverConfig.EnableTLS, "tls", "", false, "whether to enable the TLS connection")
	flag.StringVarP(&receiverConfig.CACert, "ca-crt", "", "", "the ca certificate path")
	flag.StringVarP(&receiverConfig.ClientCert, "client-crt", "", "", "the client certificate path")
	flag.StringVarP(&receiverConfig.ClientKey, "client-key", "", "", "the client key path")
	flag.StringVarP(&receiverConfig.ClientID, "client-id", "", "", "the client id for the MQTT consumer")
	flag.StringVarP(&receiverConfig.Topic, "topic", "", "", "the topic for the MQTT consumer")
	QoS := flag.IntP("QoS", "q", 0, "the level of reliability and assurance of message delivery between an MQTT client and broker")
	flag.Parse()

	if receiverConfig.Broker == "" {
		receiverConfig.Broker = os.Getenv("BROKER")
	}
	receiverConfig.QoS = byte(*QoS)

	receiveCount := 0
	choke := make(chan [2]string)

	opts := MQTT.NewClientOptions()
	opts.AddBroker(receiverConfig.Broker)
	opts.SetClientID(receiverConfig.ClientID)
	opts.SetTLSConfig(utils.NewTLSConfig(receiverConfig.CACert, receiverConfig.ClientCert,
		receiverConfig.ClientKey))
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

	for receiveCount < 4 {
		incoming := <-choke
		fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
		receiveCount++
	}
	client.Disconnect(250)
	fmt.Println("Subscriber Disconnected")
}
