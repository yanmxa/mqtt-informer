package client

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/yanmxa/mqtt-informer/pkg/config"
	"github.com/yanmxa/mqtt-informer/pkg/utils"
)

func GetClient(config *config.ClientConfig) MQTT.Client {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(config.Broker)
	opts.SetClientID(config.ClientID)
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
