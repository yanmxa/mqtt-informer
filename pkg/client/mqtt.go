package client

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/yanmxa/mqtt-informer/pkg/config"
	"github.com/yanmxa/mqtt-informer/pkg/utils"
)

func GetSender(config *config.SendConfig) MQTT.Client {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(config.Broker)
	opts.SetClientID(config.SenderID)
	opts.SetTLSConfig(utils.NewTLSConfig(config.CACert, config.ClientCert, config.ClientKey))

	c := MQTT.NewClient(opts)
	// if token := c.Connect(); token.Wait() && token.Error() != nil {
	// 	panic(token.Error())
	// }

	// fmt.Println("Sample Publisher Started")
	// i := 0
	// for range time.Tick(time.Duration(1) * time.Second) {
	// 	if i == 5 {
	// 		break
	// 	}
	// 	text := fmt.Sprintf("this is msg #%d!", i)
	// 	token := c.Publish(senderConfig.Topic, senderConfig.QoS, senderConfig.Retained, text)
	// 	token.Wait()
	// 	fmt.Println("Published: ", text)
	// 	i++
	// }
	// c.Disconnect(250)
	return c
}

func GetReceiver(config *config.ReceiveConfig) MQTT.Client {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(config.Broker)
	opts.SetClientID(config.ReceiverID)
	opts.SetTLSConfig(utils.NewTLSConfig(config.CACert, config.ClientCert,
		config.ClientKey))

	// opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
	// 	choke <- [2]string{msg.Topic(), string(msg.Payload())}
	// })

	client := MQTT.NewClient(opts)
	// if token := client.Connect(); token.Wait() && token.Error() != nil {
	// 	panic(token.Error())
	// }
	// fmt.Println("Subscriber Starting")
	// if token := client.Subscribe(receiverConfig.Topic, receiverConfig.QoS,
	// 	// func(client MQTT.Client, msg MQTT.Message) {
	// 	// 	fmt.Println("received message: ", msg.Topic(), string(msg.Payload()))
	// 	// }
	// 	nil); token.Wait() && token.Error() != nil {
	// 	fmt.Println(token.Error())
	// 	os.Exit(1)
	// }

	// for receiveCount < 4 {
	// 	incoming := <-choke
	// 	fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
	// 	receiveCount++
	// }
	// client.Disconnect(250)
	// fmt.Println("Subscriber Disconnected")
	return client
}
