package main

import (
	goflag "flag"
	"fmt"
	"os"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	flag "github.com/spf13/pflag"

	"github.com/yanmxa/mqtt-informer/pkg/config"
	"github.com/yanmxa/mqtt-informer/pkg/utils"
)

func main() {
	senderConfig := config.NewSendConfig()
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	flag.StringVarP(&senderConfig.Broker, "broker", "b", "", "the MQTT server")
	flag.BoolVarP(&senderConfig.EnableTLS, "tls", "", false, "whether to enable the TLS connection")
	flag.StringVarP(&senderConfig.CACert, "ca-crt", "", "", "the ca certificate path")
	flag.StringVarP(&senderConfig.ClientCert, "client-crt", "", "", "the client certificate path")
	flag.StringVarP(&senderConfig.ClientKey, "client-key", "", "", "the client key path")
	flag.StringVarP(&senderConfig.ClientID, "client-id", "", "", "the client id for the MQTT consumer")
	flag.StringVarP(&senderConfig.Topic, "topic", "", "", "the topic for the MQTT consumer")
	flag.BoolVarP(&senderConfig.Retained, "retained", "", false, "retain the MQTT message or not")
	QoS := flag.IntP("QoS", "q", 0, "the level of reliability and assurance of message delivery between an MQTT client and broker")

	flag.Parse()
	if senderConfig.Broker == "" {
		senderConfig.Broker = os.Getenv("BROKER")
	}
	senderConfig.QoS = byte(*QoS)

	opts := MQTT.NewClientOptions()
	opts.AddBroker(senderConfig.Broker)
	opts.SetClientID(senderConfig.ClientID)
	opts.SetTLSConfig(utils.NewTLSConfig(senderConfig.CACert, senderConfig.ClientCert, senderConfig.ClientKey))

	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	fmt.Println("Sample Publisher Started")
	i := 0
	for range time.Tick(time.Duration(1) * time.Second) {
		if i == 5 {
			break
		}
		text := fmt.Sprintf("this is msg #%d!", i)
		token := c.Publish(senderConfig.Topic, senderConfig.QoS, senderConfig.Retained, text)
		token.Wait()
		fmt.Println("Published: ", text)
		i++
	}

	c.Disconnect(250)
}
