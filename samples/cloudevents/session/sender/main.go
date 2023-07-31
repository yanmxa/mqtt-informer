package main

import (
	"context"
	"log"
	"net"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"

	mqtt_paho "github.com/cloudevents/sdk-go/protocol/mqtt_paho/v2"
	cecontext "github.com/cloudevents/sdk-go/v2/context"
)

const (
	count = 10
)

func main() {
	ctx := context.Background()
	conn, err := net.Dial("tcp", "127.0.0.1:1883")
	if err != nil {
		log.Fatalf("failed to connect to mqtt broker: %s", err.Error())
	}
	config := &paho.ClientConfig{
		ClientID: "sender-client-id",
		Conn:     conn,
	}
	// optional connect option
	connOpt := &paho.Connect{
		ClientID:   "sender-connection-id1",
		KeepAlive:  30,
		CleanStart: false,
	}
	publishOpt := &paho.Publish{
		Topic:  "test-topic",
		QoS:    1,
		Retain: false,
	}

	p, err := mqtt_paho.New(ctx, config, mqtt_paho.WithPublish(publishOpt), mqtt_paho.WithConnect(connOpt))
	if err != nil {
		log.Fatalf("failed to create protocol: %v", err)
	}
	defer p.Close(ctx)

	c, err := cloudevents.NewClient(p, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	for i := 0; i < count; i++ {
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
			cecontext.WithTopic(ctx, "test-topic"),
			e,
		); cloudevents.IsUndelivered(result) {
			log.Printf("failed to send: %v", result)
		} else {
			log.Printf("sent: %d, accepted: %t", i, cloudevents.IsACK(result))
		}
		time.Sleep(1 * time.Second)
	}
}
