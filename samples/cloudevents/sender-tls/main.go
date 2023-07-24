package main

import (
	"context"
	"crypto/tls"
	"log"
	"time"

	cemqtt "github.com/cloudevents/sdk-go/protocol/mqtt_paho/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	cecontext "github.com/cloudevents/sdk-go/v2/context"
	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"
	"github.com/yanmxa/straw/pkg/utils"
)

const (
	Broker     = "**.iot.us-east-1.amazonaws.com:8883" // "localhost:1883"
	CACert     = "./samples/cloudevents/certs/root-CA.crt"
	ClientCert = "./samples/cloudevents/certs/myan.cert.pem"
	ClientKey  = "./samples/cloudevents/certs/myan.private.key"
	Topic      = "sdk/test/python"
	ClientID   = "basicPubSub"
)

func main() {
	ctx := context.Background()
	tlsConfig := utils.NewTLSConfig(CACert, ClientCert, ClientKey)

	conn, err := tls.Dial("tcp", Broker, tlsConfig)
	if err != nil {
		log.Fatalf("failed to connect to %s: %s", Broker, err.Error())
	}

	p, err := cemqtt.New(ctx, &paho.ClientConfig{
		Conn: conn,
	}, &paho.Connect{
		ClientID:   ClientID,
		KeepAlive:  30,
		CleanStart: true,
	}, Topic, nil, 0, false)
	if err != nil {
		log.Fatalf("failed to create protocol: %s", err.Error())
	}

	c, err := cloudevents.NewClient(p, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	for i := 0; i < 10; i++ {
		e := cloudevents.NewEvent()
		e.SetID(uuid.New().String())
		e.SetType("com.cloudevents.sample.sent")
		e.SetSource("https://github.com/cloudevents/sdk-go/samples/mqtt/sender")
		err = e.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
			"id":      i,
			"message": "Hello, World!",
		})
		if err != nil {
			log.Printf("failed to set data: %v", err)
		}
		if result := c.Send(
			cecontext.WithTopic(ctx, Topic),
			e,
		); cloudevents.IsUndelivered(result) {
			log.Printf("failed to send: %v", result)
		} else {
			log.Printf("sent: %d, accepted: %t", i, cloudevents.IsACK(result))
		}
		time.Sleep(1 * time.Second)
	}
}
